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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
 *   <li><b>Refill</b> — a single dispatcher thread tops the queue up from the database, picking
 *       due tasks (including retries whose backoff has passed) by priority then age. It runs
 *       when the queue drops to the low watermark, and also at once when a task more urgent than
 *       something in memory has spilled to disk; then it uses the same eviction rule, so a
 *       spilled HIGH task displaces a LOW one instead of waiting for the queue to drain.</li>
 * </ol>
 *
 * <p>Aging inside the memory queue only helps tasks that are in memory. So that a lower
 * priority task left on disk cannot starve while urgent work keeps coming, each refill first
 * loads a few tasks that have been runnable for longer than the aging threshold, and queues them
 * at the top level.
 *
 * <p>An id stays marked as in memory from the moment it is queued until a worker's claim on it
 * has finished, so a refill does not queue it again; and a refill whose database read began
 * before that claim finished does not load it either (the read may predate the claim). If a
 * stale copy still slips in, it is harmless: claiming is a conditional update, so the second
 * copy is skipped as a claim miss.
 *
 * <p>The dispatcher thread is stopped with a flag and never interrupted: an interrupt landing
 * inside an H2 file write can close the database's file channel.
 */
@Component
public class TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatcher.class);
    private static final int NONE = Integer.MAX_VALUE;

    private final BoundedPriorityBlockingQueue<QueuedTask> queue;
    private final Set<String> inMemory = ConcurrentHashMap.newKeySet();
    private final TaskStore store;
    private final Clock clock;
    private final TaskMetrics metrics;
    private final int lowWatermark;
    private final long pollIntervalMillis;
    private final Duration agingThreshold;
    private final int starvedPerRefill;
    /**
     * When each id was last released by a worker (a sequence number, not a time). A refill skips
     * an id released after its database read began: that read may show the row as PENDING from
     * just before the worker's claim committed, and loading it again would only make a stale copy.
     */
    private final Map<String, Long> releasedAt = new ConcurrentHashMap<>();
    private final AtomicLong releaseSeq = new AtomicLong();
    /** Most urgent level that has spilled to disk since the last refill; NONE when nothing has. */
    private final AtomicInteger mostUrgentSpilled = new AtomicInteger(NONE);
    /** Bumped on every start, so a refill thread left over from an earlier run exits instead of running alongside. */
    private final AtomicInteger feederRun = new AtomicInteger();
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
        this.agingThreshold = properties.queue().agingThreshold();
        this.starvedPerRefill = Math.max(1, capacity / 8);
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
        open();
        int run = feederRun.incrementAndGet();
        Thread t = new Thread(() -> feedLoop(run), "task-dispatcher");
        t.setDaemon(true);
        feeder = t;
        t.start();
    }

    /** Lets the fast path and the refill put tasks into memory (again, after a stop). */
    void open() {
        queue.reopen();
        feeding = true;
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
            spilled(priorityLevel);
            return;
        }
        result.evictedElement().ifPresent(evicted -> {
            inMemory.remove(evicted.id());
            spilled(evicted.priorityLevel());
        });
    }

    private void spilled(int level) {
        metrics.spilledToDisk.increment();
        mostUrgentSpilled.accumulateAndGet(level, Math::min);
        if (level < queue.leastUrgentLevelPresent()) {
            wake();
        }
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
        releasedAt.put(id, releaseSeq.incrementAndGet());
        inMemory.remove(id);
    }

    /**
     * Closes the memory queue and empties it. The returned tasks are not lost: each one is
     * still PENDING or RETRY_WAIT in the database and will be loaded again after a restart.
     */
    public List<String> closeAndDrain() {
        feeding = false;
        queue.close();
        List<String> ids = queue.drain().stream().map(QueuedTask::id).toList();
        inMemory.clear();
        releasedAt.clear();
        return ids;
    }

    /**
     * Loads due tasks from the database when the queue is at or below the low watermark, or when
     * a task more urgent than the least urgent one in memory has spilled to disk.
     */
    int refill() {
        // Releases up to now are reflected in any read that starts from here on, so they no longer
        // matter. Trimming on every call, not only when a read happens, keeps this log to roughly
        // one poll interval of releases even while the queue stays above the low watermark.
        long readStartedAfter = releaseSeq.get();
        releasedAt.values().removeIf(seq -> seq <= readStartedAfter);

        int size = queue.size();
        boolean low = size <= lowWatermark;
        boolean urgentOnDisk = mostUrgentSpilled.get() < queue.leastUrgentLevelPresent();
        if (!low && !urgentOnDisk) {
            return 0;
        }
        int spilledBefore = mostUrgentSpilled.getAndSet(NONE);
        try {
            return loadDue(clock.instant(), readStartedAfter);
        } catch (RuntimeException e) {
            // Keep the reason for the urgent refill, so the next poll tries again.
            mostUrgentSpilled.accumulateAndGet(spilledBefore, Math::min);
            throw e;
        }
    }

    /** How many worker releases are being remembered for stale-read checks (for tests). */
    int releasesTracked() {
        return releasedAt.size();
    }

    private int loadDue(Instant now, long readStartedAfter) {
        int loaded = 0;

        // Tasks that have waited past the aging threshold go first, promoted to the top level.
        // A threshold of zero or less turns aging off here too, as it does in the memory queue.
        if (!agingThreshold.isNegative() && !agingThreshold.isZero()) {
            for (DispatchCandidate c : store.findStarved(now.minus(agingThreshold), starvedPerRefill)) {
                Load result = load(c.id(), 0, readStartedAfter);
                if (result == Load.REJECTED) {
                    break;
                }
                loaded += result == Load.LOADED ? 1 : 0;
            }
        }

        // Then by priority and age. Ask for extra rows: the most urgent ones are often already in memory.
        int capacity = queue.capacity();
        for (DispatchCandidate c : store.findDispatchable(now, capacity + queue.size())) {
            Load result = load(c.id(), c.priority(), readStartedAfter);
            if (result == Load.REJECTED) {
                break;
            }
            loaded += result == Load.LOADED ? 1 : 0;
        }
        return loaded;
    }

    /**
     * Puts one task from the database into memory, evicting a less urgent one if memory is full.
     * REJECTED means it could not get in, so no later candidate (equally or less urgent) would either.
     */
    private Load load(String id, int level, long readStartedAfter) {
        Long released = releasedAt.get(id);
        if (released != null && released > readStartedAfter) {
            return Load.STALE;
        }
        if (!inMemory.add(id)) {
            return Load.ALREADY_IN_MEMORY;
        }
        var result = queue.offerOrEvict(new QueuedTask(id, level));
        if (!result.accepted()) {
            inMemory.remove(id);
            return Load.REJECTED;
        }
        result.evictedElement().ifPresent(evicted -> {
            inMemory.remove(evicted.id());
            metrics.spilledToDisk.increment();
        });
        return Load.LOADED;
    }

    private void feedLoop(int run) {
        while (feeding && feederRun.get() == run) {
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

    private enum Load { LOADED, ALREADY_IN_MEMORY, STALE, REJECTED }
}
