package io.github.yq808.taskqueue.engine;

import io.github.yq808.taskqueue.config.TaskQueueProperties;
import io.github.yq808.taskqueue.domain.TaskPriority;
import io.github.yq808.taskqueue.metrics.TaskMetrics;
import io.github.yq808.taskqueue.persistence.DispatchCandidate;
import io.github.yq808.taskqueue.persistence.TaskStore;
import io.github.yq808.taskqueue.queue.BoundedPriorityBlockingQueue;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Owns the bounded in-memory queue and keeps it fed.
 *
 * <p>Two ways in:
 * <ol>
 *   <li><b>Fast path</b> — right after a submission commits, {@link #offerNew} puts it straight
 *       into memory. If memory is full, a more urgent task evicts the newest, least urgent one;
 *       otherwise the new task just stays on disk. Nothing is lost either way because the row is
 *       already PENDING in the database.</li>
 *   <li><b>Refill</b> — a single dispatcher thread tops the queue up from the database whenever
 *       it drops to the low watermark, picking due tasks (including retries whose backoff has
 *       passed) by priority then age.</li>
 * </ol>
 *
 * <p>An id stays marked as in memory from the moment it is queued until a worker's claim on it
 * has finished, so a refill does not queue it again. If a stale copy still slips in (for example
 * a refill query that read the row just before a redrive), it is harmless: claiming is a
 * conditional update, so the second copy is skipped as a claim miss.
 *
 * <p>The dispatcher thread is stopped with a flag and never interrupted: an interrupt landing
 * inside an H2 file write can close the database's file channel.
 */
@Component
public class TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatcher.class);

    private final BoundedPriorityBlockingQueue<QueuedTask> queue;
    private final Set<String> inMemory = ConcurrentHashMap.newKeySet();
    private final TaskStore store;
    private final Clock clock;
    private final TaskMetrics metrics;
    private final int lowWatermark;
    private final long pollIntervalMillis;
    private final Object wakeLock = new Object();
    private boolean wakeRequested;

    private volatile boolean feeding;
    private volatile Thread feeder;

    public TaskDispatcher(TaskQueueProperties properties, TaskStore store, Clock clock, TaskMetrics metrics,
                          MeterRegistry registry) {
        int capacity = properties.queue().effectiveCapacity(properties.worker().threads());
        this.queue = new BoundedPriorityBlockingQueue<>(capacity, TaskPriority.levelCount(),
                properties.queue().agingThreshold().toNanos());
        this.lowWatermark = (int) Math.floor(capacity * properties.queue().lowWatermarkRatio());
        this.pollIntervalMillis = Math.max(10, properties.dispatcher().pollInterval().toMillis());
        this.store = store;
        this.clock = clock;
        this.metrics = metrics;
        Gauge.builder("taskqueue.queue.size", queue, BoundedPriorityBlockingQueue::size)
                .description("tasks in the in-memory queue").register(registry);
        Gauge.builder("taskqueue.queue.capacity", queue, BoundedPriorityBlockingQueue::capacity)
                .description("in-memory queue capacity").register(registry);
        Gauge.builder("taskqueue.queue.peak", queue, BoundedPriorityBlockingQueue::peakSize)
                .description("largest in-memory queue size seen").register(registry);
    }

    public void startFeeding() {
        feeding = true;
        Thread t = new Thread(this::feedLoop, "task-dispatcher");
        t.setDaemon(true);
        feeder = t;
        t.start();
    }

    /** Stops the refill thread (flag + wake, no interrupt) and waits for it. */
    public void stopFeeding(Duration wait) throws InterruptedException {
        feeding = false;
        wake();
        Thread t = feeder;
        if (t != null) {
            t.join(wait.toMillis());
        }
    }

    /** Fast path for a task that has just been committed as PENDING. */
    public void offerNew(String id, int priorityLevel) {
        if (!feeding || !inMemory.add(id)) {
            return;
        }
        var result = queue.offerOrEvict(new QueuedTask(id, priorityLevel));
        if (!result.accepted()) {
            inMemory.remove(id);
            metrics.spilledToDisk.increment();
            return;
        }
        result.evictedElement().ifPresent(evicted -> {
            inMemory.remove(evicted.id());
            metrics.spilledToDisk.increment();
        });
    }

    public QueuedTask poll(long timeout, TimeUnit unit) throws InterruptedException {
        QueuedTask task = queue.poll(timeout, unit);
        if (task != null && queue.size() <= lowWatermark) {
            wake();
        }
        return task;
    }

    /**
     * Called once a worker's claim attempt for a polled task has finished. Until then the id
     * stays marked as in memory, so a refill cannot queue it a second time while the claim is
     * still in flight (the row would still read PENDING).
     */
    public void forget(String id) {
        inMemory.remove(id);
    }

    /**
     * Closes the memory queue and empties it. The returned tasks are not lost: each one is
     * still PENDING or RETRY_WAIT in the database and will be loaded again after a restart.
     */
    public List<String> closeAndDrain() {
        queue.close();
        List<String> ids = queue.drain().stream().map(QueuedTask::id).toList();
        inMemory.clear();
        return ids;
    }

    /** Loads due tasks from the database when the queue is at or below the low watermark. */
    int refill() {
        int size = queue.size();
        if (size > lowWatermark) {
            return 0;
        }
        int free = queue.capacity() - size;
        if (free <= 0) {
            return 0;
        }
        // Ask for extra rows: the most urgent rows are often the ones already in memory.
        List<DispatchCandidate> candidates = store.findDispatchable(clock.instant(), free + size);
        int loaded = 0;
        for (DispatchCandidate c : candidates) {
            if (loaded >= free) {
                break;
            }
            if (!inMemory.add(c.id())) {
                continue;
            }
            if (!queue.offer(new QueuedTask(c.id(), c.priority()))) {
                inMemory.remove(c.id());
                break;
            }
            loaded++;
        }
        return loaded;
    }

    private void feedLoop() {
        while (feeding) {
            try {
                refill();
            } catch (RuntimeException e) {
                log.warn("Refilling the queue from the database failed; will retry", e);
            }
            awaitWake(pollIntervalMillis);
        }
    }

    private void wake() {
        synchronized (wakeLock) {
            wakeRequested = true;
            wakeLock.notifyAll();
        }
    }

    private void awaitWake(long millis) {
        synchronized (wakeLock) {
            if (!wakeRequested && feeding) {
                try {
                    wakeLock.wait(millis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    feeding = false;
                }
            }
            wakeRequested = false;
        }
    }

    public int size() {
        return queue.size();
    }

    public int capacity() {
        return queue.capacity();
    }

    public int peakSize() {
        return queue.peakSize();
    }

    public int lowWatermark() {
        return lowWatermark;
    }

    public int[] sizeByPriority() {
        return queue.sizeByLevel();
    }

    public long oldestQueuedMillis() {
        return queue.oldestAgeNanos().map(TimeUnit.NANOSECONDS::toMillis).orElse(0L);
    }
}
