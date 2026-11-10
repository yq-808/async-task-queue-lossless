package io.github.yq808.taskqueue.engine;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.github.yq808.taskqueue.config.TaskQueueProperties;
import io.github.yq808.taskqueue.persistence.ClaimedTask;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * A fixed set of worker threads, each looping: take from the memory queue, process, repeat.
 * The pool's own work queue is a {@link SynchronousQueue} because it only ever receives the
 * worker loops themselves; the task backlog lives in {@link TaskDispatcher} and the database.
 *
 * <p>Stopping is two-phase so the pool converges within a known time: first no new tasks are
 * taken and running ones get {@code awaitRunning} to finish; then the ones still inside a
 * handler are interrupted, write themselves back as PENDING, and get {@code awaitInterrupted}.
 */
@Component
public class WorkerPool {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);
    private static final long POLL_MILLIS = 200;

    private final int threads;
    private final TaskDispatcher dispatcher;
    private final TaskProcessor processor;
    private final DownstreamCircuitBreaker breaker;
    private final String instanceId = UUID.randomUUID().toString().substring(0, 8);
    private final AtomicInteger busy = new AtomicInteger();
    private final List<WorkerSlot> slots = new CopyOnWriteArrayList<>();

    private volatile boolean running;
    private volatile ThreadPoolExecutor executor;

    public WorkerPool(TaskQueueProperties properties, TaskDispatcher dispatcher, TaskProcessor processor,
                      DownstreamCircuitBreaker breaker, MeterRegistry registry) {
        this.threads = properties.worker().threads();
        this.dispatcher = dispatcher;
        this.processor = processor;
        this.breaker = breaker;
        Gauge.builder("taskqueue.workers.active", busy, AtomicInteger::get)
                .description("workers running a task right now").register(registry);
        Gauge.builder("taskqueue.workers.threads", () -> threads)
                .description("configured worker threads").register(registry);
    }

    public void start() {
        slots.clear();
        running = true;
        executor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
                new ThreadFactoryBuilder().setNameFormat("task-worker-%d").build(),
                new ThreadPoolExecutor.AbortPolicy());
        for (int i = 0; i < threads; i++) {
            WorkerSlot slot = new WorkerSlot(instanceId + "/worker-" + i);
            slots.add(slot);
            executor.execute(() -> loop(slot));
        }
    }

    public StopReport stop(Duration awaitRunning, Duration awaitInterrupted) throws InterruptedException {
        running = false;
        ThreadPoolExecutor pool = executor;
        if (pool == null) {
            return new StopReport(true, 0, 0);
        }
        pool.shutdown(); // no interrupts yet: let in-flight tasks finish on their own
        if (pool.awaitTermination(awaitRunning.toMillis(), TimeUnit.MILLISECONDS)) {
            return new StopReport(true, 0, 0);
        }
        int interrupted = 0;
        for (WorkerSlot slot : slots) {
            if (slot.interruptIfInHandler()) {
                interrupted++;
            }
        }
        log.warn("{} task(s) still running after {}; interrupted them so they write themselves back",
                interrupted, awaitRunning);
        boolean done = pool.awaitTermination(awaitInterrupted.toMillis(), TimeUnit.MILLISECONDS);
        return new StopReport(done, interrupted, done ? 0 : pool.getActiveCount());
    }

    private void loop(WorkerSlot slot) {
        slot.bind(Thread.currentThread());
        while (running) {
            long breakerWait = breaker.remainingOpenNanos();
            if (breakerWait > 0) {
                LockSupport.parkNanos(Math.min(breakerWait, TimeUnit.MILLISECONDS.toNanos(POLL_MILLIS)));
                continue;
            }
            QueuedTask task;
            try {
                task = dispatcher.poll(POLL_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                continue;
            }
            if (task == null) {
                continue;
            }
            if (!running) {
                // Stopping: leave it. The row is still PENDING on disk and is picked up after restart.
                dispatcher.forget(task.id());
                break;
            }
            Optional<ClaimedTask> claimed;
            try {
                claimed = processor.claim(task, slot);
            } catch (RuntimeException e) {
                log.warn("Worker {} could not claim task {}; it stays PENDING and will be retried", slot.name(),
                        task.id(), e);
                continue;
            } finally {
                dispatcher.forget(task.id());
            }
            if (claimed.isEmpty()) {
                continue;
            }
            busy.incrementAndGet();
            try {
                processor.execute(claimed.get(), slot);
            } catch (VirtualMachineError e) {
                log.error("Worker {} hit a fatal JVM error", slot.name(), e);
            } catch (RuntimeException e) {
                // Most likely the database refused the outcome write. The task stays RUNNING and the
                // lease reaper hands it out again once the lease expires: at-least-once, not lost.
                log.error("Worker {} could not record the outcome of task {}", slot.name(), task.id(), e);
            } finally {
                busy.decrementAndGet();
            }
        }
    }

    public int activeCount() {
        return busy.get();
    }

    public int threads() {
        return threads;
    }

    /**
     * @param graceful    every worker thread ended
     * @param interrupted tasks interrupted after the grace period (each wrote itself back as PENDING)
     * @param stillAlive  threads still running after both waits; their tasks stay RUNNING and are recovered on next start
     */
    public record StopReport(boolean graceful, int interrupted, int stillAlive) {
    }
}
