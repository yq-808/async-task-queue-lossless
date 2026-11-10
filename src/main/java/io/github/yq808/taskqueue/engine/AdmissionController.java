package io.github.yq808.taskqueue.engine;

import io.github.yq808.taskqueue.config.TaskQueueProperties;
import io.github.yq808.taskqueue.domain.TaskPriority;
import io.github.yq808.taskqueue.metrics.TaskMetrics;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Backpressure at the front door. The in-memory queue is bounded, but memory is not where
 * accepted work piles up: anything that does not fit stays on disk as PENDING. What has to be
 * bounded is therefore the <b>persisted backlog</b> (PENDING + RETRY_WAIT), and this class caps
 * it before a task is written.
 *
 * <ul>
 *   <li>Priority-aware load shedding: LOW is refused first (at {@code lowShare} of the cap),
 *       then NORMAL, and HIGH only at the full cap.</li>
 *   <li>Adaptive cap: {@code throughput * targetMaxQueueWait}, clamped to
 *       [{@code minBacklog}, {@code maxBacklog}], so an accepted task can still be served within
 *       the target wait even when the downstream slows down.</li>
 *   <li>A refusal carries Retry-After: how long the current throughput needs to drain the excess.</li>
 * </ul>
 *
 * <p>The backlog figure is the last database count plus admissions since, re-synced every few
 * hundred milliseconds, and admission reserves a slot with a CAS. Between re-syncs the figure
 * only over-counts (claims are not subtracted until the next re-sync), so the cap errs on the
 * side of refusing.
 */
@Component
public class AdmissionController {

    private final TaskQueueProperties.Admission config;
    private final DoubleSupplier throughput;
    private final AtomicLong backlog = new AtomicLong();
    private volatile boolean accepting;

    @Autowired
    public AdmissionController(TaskQueueProperties properties, TaskMetrics metrics) {
        this(properties.admission(), metrics::throughputPerSecond);
    }

    AdmissionController(TaskQueueProperties.Admission config, DoubleSupplier throughput) {
        this.config = config;
        this.throughput = throughput;
    }

    public Decision tryAdmit(TaskPriority priority) {
        if (!accepting) {
            return Decision.shuttingDown();
        }
        long threshold = threshold(priority);
        while (true) {
            long current = backlog.get();
            if (current >= threshold) {
                return Decision.overloaded(retryAfterSeconds(current - threshold + 1), current, threshold);
            }
            if (backlog.compareAndSet(current, current + 1)) {
                return Decision.admit();
            }
        }
    }

    /** Gives back a slot reserved by {@link #tryAdmit} that did not turn into a new task (duplicate, error). */
    public void release() {
        backlog.updateAndGet(v -> Math.max(0, v - 1));
    }

    public void resync(long persistedBacklog) {
        backlog.set(persistedBacklog);
    }

    public void open() {
        accepting = true;
    }

    public void close() {
        accepting = false;
    }

    public boolean isAccepting() {
        return accepting;
    }

    public long backlog() {
        return backlog.get();
    }

    /** The cap currently in force, before the per-priority share is applied. */
    public long currentLimit() {
        double rate = throughput.getAsDouble();
        if (!config.adaptive() || rate <= 0) {
            return config.maxBacklog();
        }
        long adaptive = (long) Math.ceil(rate * config.targetMaxQueueWait().toSeconds());
        return Math.max(Math.min(config.minBacklog(), config.maxBacklog()), Math.min(adaptive, config.maxBacklog()));
    }

    long threshold(TaskPriority priority) {
        double share = switch (priority) {
            case HIGH -> 1.0;
            case NORMAL -> config.normalShare();
            case LOW -> config.lowShare();
        };
        return Math.max(1, (long) Math.floor(currentLimit() * share));
    }

    long retryAfterSeconds(long excess) {
        double rate = throughput.getAsDouble();
        if (rate <= 0) {
            return 5;
        }
        return Math.max(1, Math.min(300, (long) Math.ceil(excess / rate)));
    }

    public record Decision(Outcome outcome, long retryAfterSeconds, long backlog, long threshold) {

        public enum Outcome { ADMITTED, OVERLOADED, SHUTTING_DOWN }

        static Decision admit() {
            return new Decision(Outcome.ADMITTED, 0, 0, 0);
        }

        static Decision overloaded(long retryAfterSeconds, long backlog, long threshold) {
            return new Decision(Outcome.OVERLOADED, retryAfterSeconds, backlog, threshold);
        }

        static Decision shuttingDown() {
            return new Decision(Outcome.SHUTTING_DOWN, 10, 0, 0);
        }

        public boolean admitted() {
            return outcome == Outcome.ADMITTED;
        }
    }
}
