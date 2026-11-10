package io.github.yq808.taskqueue.engine;

import io.github.yq808.taskqueue.config.TaskQueueProperties;
import io.github.yq808.taskqueue.metrics.TaskMetrics;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * When the downstream keeps failing with system faults, workers stop taking tasks for a while
 * instead of turning every queued task into another failed attempt and another retry. When the
 * open period ends every worker resumes (there is no single trial request); if the downstream is
 * still failing, the next run of consecutive failures opens it again, and any success resets the
 * count.
 */
@Component
public class DownstreamCircuitBreaker {

    private final int threshold;
    private final long openNanos;
    private final LongSupplier nanoClock;
    private final TaskMetrics metrics;
    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    private volatile long openUntil = Long.MIN_VALUE;

    @Autowired
    public DownstreamCircuitBreaker(TaskQueueProperties properties, TaskMetrics metrics) {
        this(properties.execution().breakerFailureThreshold(), properties.execution().breakerOpenDuration().toNanos(),
                System::nanoTime, metrics);
    }

    DownstreamCircuitBreaker(int threshold, long openNanos, LongSupplier nanoClock, TaskMetrics metrics) {
        this.threshold = Math.max(1, threshold);
        this.openNanos = openNanos;
        this.nanoClock = nanoClock;
        this.metrics = metrics;
    }

    public void onSuccess() {
        consecutiveFailures.set(0);
    }

    public void onSystemFailure() {
        if (consecutiveFailures.incrementAndGet() >= threshold) {
            consecutiveFailures.set(0);
            openUntil = nanoClock.getAsLong() + openNanos;
            metrics.breakerOpened.increment();
        }
    }

    /** Nanoseconds left before workers may take tasks again; 0 when closed. */
    public long remainingOpenNanos() {
        long until = openUntil;
        if (until == Long.MIN_VALUE) {
            return 0;
        }
        return Math.max(0, until - nanoClock.getAsLong());
    }

    public boolean isOpen() {
        return remainingOpenNanos() > 0;
    }
}
