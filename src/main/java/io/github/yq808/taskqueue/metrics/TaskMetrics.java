package io.github.yq808.taskqueue.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * Counters and timers, registered in Micrometer (so they show up under /actuator/metrics and
 * /actuator/prometheus) and also read back for the /api/metrics summary.
 */
@Component
public class TaskMetrics {

    public final Counter submitted;
    public final Counter duplicates;
    public final Counter conflicts;
    public final Counter rejectedBacklog;
    public final Counter rejectedShutdown;
    public final Counter succeeded;
    public final Counter failed;
    public final Counter deadLettered;
    public final Counter retriesScheduled;
    public final Counter systemRetries;
    public final Counter recovered;
    public final Counter writtenBack;
    public final Counter spilledToDisk;
    public final Counter claimMisses;
    public final Counter fencedOut;
    public final Counter breakerOpened;

    private final Timer queueWait;
    private final Timer processing;

    /** Finished tasks per second, smoothed. Feeds the adaptive admission limit and Retry-After. */
    private final AtomicLong completionsSinceTick = new AtomicLong();
    private volatile double throughputPerSecond;
    private static final double EWMA_ALPHA = 0.3;

    public TaskMetrics(MeterRegistry registry) {
        submitted = counter(registry, "taskqueue.tasks.submitted", "accepted submissions");
        duplicates = counter(registry, "taskqueue.tasks.duplicates", "submissions answered with an existing task");
        conflicts = counter(registry, "taskqueue.tasks.conflicts", "same business key, different payload");
        rejectedBacklog = Counter.builder("taskqueue.tasks.rejected").tag("reason", "backlog")
                .description("refused by backpressure").register(registry);
        rejectedShutdown = Counter.builder("taskqueue.tasks.rejected").tag("reason", "shutdown")
                .description("refused while stopping").register(registry);
        succeeded = counter(registry, "taskqueue.tasks.succeeded", "finished successfully");
        failed = counter(registry, "taskqueue.tasks.failed", "failed without retry (business fatal)");
        deadLettered = counter(registry, "taskqueue.tasks.dead_lettered", "retries exhausted or system fatal");
        retriesScheduled = counter(registry, "taskqueue.tasks.retries", "business retries scheduled");
        systemRetries = counter(registry, "taskqueue.tasks.system_retries", "re-runs after recoverable system faults");
        recovered = counter(registry, "taskqueue.tasks.recovered", "orphaned RUNNING tasks recovered");
        writtenBack = counter(registry, "taskqueue.tasks.written_back", "running tasks handed back by shutdown");
        spilledToDisk = counter(registry, "taskqueue.queue.spilled", "tasks left on disk because memory was full");
        claimMisses = counter(registry, "taskqueue.queue.claim_misses", "stale queue entries skipped");
        fencedOut = counter(registry, "taskqueue.tasks.fenced_out", "late results discarded after a lost lease");
        breakerOpened = counter(registry, "taskqueue.breaker.opened", "downstream circuit breaker openings");
        queueWait = Timer.builder("taskqueue.queue.wait").description("time a task waited while runnable")
                .publishPercentiles(0.5, 0.95, 0.99).register(registry);
        processing = Timer.builder("taskqueue.task.processing").description("time spent running an attempt")
                .publishPercentiles(0.5, 0.95, 0.99).register(registry);
    }

    private static Counter counter(MeterRegistry registry, String name, String description) {
        return Counter.builder(name).description(description).register(registry);
    }

    public void recordQueueWait(long millis) {
        queueWait.record(millis, TimeUnit.MILLISECONDS);
    }

    public void recordProcessing(long millis) {
        processing.record(millis, TimeUnit.MILLISECONDS);
    }

    /** A task left the backlog for good (succeeded, failed or dead-lettered). */
    public void onTaskFinished() {
        completionsSinceTick.incrementAndGet();
    }

    /** Called once per second while there is work; idle seconds are skipped so the rate does not decay to zero. */
    public void tickThroughput(double elapsedSeconds) {
        double instant = completionsSinceTick.getAndSet(0) / Math.max(elapsedSeconds, 1e-3);
        throughputPerSecond = throughputPerSecond == 0
                ? instant
                : EWMA_ALPHA * instant + (1 - EWMA_ALPHA) * throughputPerSecond;
    }

    public double throughputPerSecond() {
        return throughputPerSecond;
    }

    public TimerSummary queueWaitSummary() {
        return summarize(queueWait);
    }

    public TimerSummary processingSummary() {
        return summarize(processing);
    }

    private static TimerSummary summarize(Timer timer) {
        var snapshot = timer.takeSnapshot();
        double p95 = 0;
        for (ValueAtPercentile v : snapshot.percentileValues()) {
            if (Math.abs(v.percentile() - 0.95) < 1e-9) {
                p95 = v.value(TimeUnit.MILLISECONDS);
            }
        }
        double max = snapshot.max(TimeUnit.MILLISECONDS);
        // The percentile comes from a histogram estimate and can land just above the exact max.
        return new TimerSummary(snapshot.count(), snapshot.mean(TimeUnit.MILLISECONDS), Math.min(p95, max), max);
    }

    public record TimerSummary(long count, double avgMs, double p95Ms, double maxMs) {

        public Duration average() {
            return Duration.ofMillis(Math.round(avgMs));
        }
    }
}
