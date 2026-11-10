package io.github.yq808.taskqueue.engine;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.github.yq808.taskqueue.config.TaskQueueProperties;
import io.github.yq808.taskqueue.metrics.TaskMetrics;
import io.github.yq808.taskqueue.persistence.TaskStore;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Starts and stops the whole pipeline in a safe order.
 *
 * <p>Start: recover tasks the previous process left RUNNING, re-sync the backlog count, start
 * the dispatcher and the workers, schedule the maintenance jobs, and only then open admission.
 *
 * <p>Stop (SIGTERM, Ctrl-C, context close), in this order:
 * <ol>
 *   <li>close admission: new submissions get 503 + Retry-After; queries keep working</li>
 *   <li>stop the dispatcher refilling memory from disk</li>
 *   <li>stop the workers: running tasks get {@code awaitRunning} to finish, then those still in a
 *       handler are interrupted and write themselves back as PENDING</li>
 *   <li>close and drain the memory queue, and check that every drained task is still waiting in
 *       the database (it always should be: a task is written before it is queued)</li>
 *   <li>stop maintenance jobs</li>
 * </ol>
 * The phase is the highest, so this runs before the web server begins its own graceful
 * shutdown, and before the DataSource closes.
 *
 * <p>This assumes a single instance per database: at start, every RUNNING row belongs to a
 * previous process that is gone. Several instances would instead recover only rows whose lease
 * has expired, which the periodic lease reaper already does.
 */
@Component
public class TaskEngine implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(TaskEngine.class);
    private static final Instant FAR_FUTURE = Instant.parse("9999-12-31T00:00:00Z");
    private static final int PURGE_BATCH = 500;

    private final TaskQueueProperties properties;
    private final TaskStore store;
    private final AdmissionController admission;
    private final TaskDispatcher dispatcher;
    private final WorkerPool workers;
    private final TaskMetrics metrics;
    private final Clock clock;

    private volatile boolean running;
    private volatile boolean stopRequested;
    private volatile ScheduledExecutorService maintenance;
    private volatile ScheduledExecutorService retention;
    private volatile long lastTickNanos;
    private volatile ShutdownReport lastShutdown;

    public TaskEngine(TaskQueueProperties properties, TaskStore store, AdmissionController admission,
                      TaskDispatcher dispatcher, WorkerPool workers, TaskMetrics metrics, Clock clock,
                      MeterRegistry registry) {
        this.properties = properties;
        this.store = store;
        this.admission = admission;
        this.dispatcher = dispatcher;
        this.workers = workers;
        this.metrics = metrics;
        this.clock = clock;
        Gauge.builder("taskqueue.backlog", admission, AdmissionController::backlog)
                .description("persisted tasks waiting to run (PENDING + RETRY_WAIT)").register(registry);
        Gauge.builder("taskqueue.admission.limit", admission, AdmissionController::currentLimit)
                .description("backlog cap currently in force").register(registry);
        Gauge.builder("taskqueue.throughput", metrics, TaskMetrics::throughputPerSecond)
                .description("finished tasks per second (EWMA)").register(registry);
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        stopRequested = false;
        Instant now = clock.instant();
        TaskStore.RecoveryResult recovered = store.recoverExpiredLeases(FAR_FUTURE, now);
        if (recovered.total() > 0) {
            metrics.recovered.increment(recovered.total());
            log.warn("Recovered {} task(s) left RUNNING by the previous process: {} requeued, {} dead-lettered",
                    recovered.total(), recovered.requeued(), recovered.deadLettered());
        }
        admission.resync(store::countBacklog);

        dispatcher.startFeeding();
        workers.start();

        maintenance = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("task-maintenance").setDaemon(true).build());
        long leaseCheck = properties.execution().leaseCheckInterval().toMillis();
        maintenance.scheduleWithFixedDelay(guard("lease reaper", this::reapExpiredLeases), leaseCheck, leaseCheck,
                TimeUnit.MILLISECONDS);
        maintenance.scheduleWithFixedDelay(guard("backlog resync", this::resyncBacklog), 500, 500,
                TimeUnit.MILLISECONDS);
        lastTickNanos = System.nanoTime();
        maintenance.scheduleAtFixedRate(guard("throughput", this::tickThroughput), 1, 1, TimeUnit.SECONDS);
        // Cleanup gets its own thread: a large purge must not hold up the lease reaper or the
        // backlog re-sync (without re-syncs the admission estimate only grows and refuses work).
        retention = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("task-retention").setDaemon(true).build());
        long purge = properties.retention().cleanupInterval().toMillis();
        retention.scheduleWithFixedDelay(guard("retention", this::purgeOldResults), purge, purge,
                TimeUnit.MILLISECONDS);

        admission.open();
        running = true;
        log.info("Task engine started: {} workers, memory queue capacity {} (low watermark {}), backlog {} / cap {}",
                workers.threads(), dispatcher.capacity(), dispatcher.lowWatermark(), admission.backlog(),
                admission.currentLimit());
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        stopRequested = true;
        long startNanos = System.nanoTime();
        log.info("Task engine stopping: refusing new submissions");
        admission.close();
        try {
            dispatcher.stopFeeding(Duration.ofSeconds(2));
            WorkerPool.StopReport workerReport = workers.stop(properties.shutdown().awaitRunning(),
                    properties.shutdown().awaitInterrupted());
            List<String> drained = dispatcher.closeAndDrain();
            long durable = drained.isEmpty() ? 0 : store.countWaiting(drained);
            stopJobs();
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            lastShutdown = new ShutdownReport(elapsedMs, drained.size(), durable, workerReport.interrupted(),
                    workerReport.stillAlive());
            log.info("Task engine stopped in {} ms: {} queued task(s) drained from memory, {} confirmed waiting in H2 "
                            + "(the rest had already finished), {} running task(s) interrupted and written back, "
                            + "{} worker(s) still alive (their tasks are recovered on next start)",
                    elapsedMs, drained.size(), durable, workerReport.interrupted(), workerReport.stillAlive());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while stopping; anything unfinished is recovered on next start");
        } finally {
            stopJobs(); // also on the interrupted path, so a later start() does not run every job twice
            running = false;
        }
    }

    /** Stops the maintenance and retention jobs without interrupting them: a job may be mid-write. */
    private void stopJobs() {
        for (ScheduledExecutorService jobs : new ScheduledExecutorService[] {maintenance, retention}) {
            if (jobs == null || jobs.isShutdown()) {
                continue;
            }
            jobs.shutdown();
            if (!Thread.currentThread().isInterrupted()) {
                try {
                    jobs.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    public ShutdownReport lastShutdown() {
        return lastShutdown;
    }

    void reapExpiredLeases() {
        Instant now = clock.instant();
        TaskStore.RecoveryResult r = store.recoverExpiredLeases(now, now);
        admission.returnedToBacklog(r.requeued());
        if (r.total() > 0) {
            metrics.recovered.increment(r.total());
            log.warn("Lease reaper: {} task(s) requeued, {} dead-lettered", r.requeued(), r.deadLettered());
        }
    }

    void resyncBacklog() {
        admission.resync(store::countBacklog);
    }

    void tickThroughput() {
        long now = System.nanoTime();
        double elapsed = (now - lastTickNanos) / 1e9;
        lastTickNanos = now;
        if (admission.backlog() > 0 || workers.activeCount() > 0) {
            metrics.tickThroughput(elapsed);
        }
    }

    void purgeOldResults() {
        Instant cutoff = clock.instant().minus(properties.retention().succeededTtl());
        int total = 0;
        int n;
        while (!stopRequested && (n = store.purgeSucceededBefore(cutoff, PURGE_BATCH)) > 0) {
            total += n;
        }
        if (total > 0) {
            log.info("Retention: deleted {} succeeded task(s) finished before {}", total, cutoff);
        }
    }

    private static Runnable guard(String name, Runnable job) {
        return () -> {
            try {
                job.run();
            } catch (RuntimeException e) {
                log.warn("Maintenance job '{}' failed; it will run again", name, e);
            }
        };
    }

    /**
     * @param drainedFromMemory   ids that were in the memory queue when it was closed
     * @param confirmedWaiting    how many of those are PENDING/RETRY_WAIT in H2 (the rest had already finished)
     * @param interruptedRunning  running tasks interrupted after the grace period and written back as PENDING
     * @param workersStillAlive   workers that did not stop in time
     */
    public record ShutdownReport(long elapsedMs, int drainedFromMemory, long confirmedWaiting,
                                 int interruptedRunning, int workersStillAlive) {
    }
}
