package io.github.yq808.taskqueue.engine;

import io.github.yq808.taskqueue.config.TaskQueueProperties;
import io.github.yq808.taskqueue.domain.TaskPriority;
import io.github.yq808.taskqueue.metrics.TaskMetrics;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
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
 * <p>The backlog figure is an estimate that is <b>never below</b> the real number of waiting
 * rows, so new admissions can never push the real backlog past the cap:
 * <ul>
 *   <li>admitting reserves a slot (counted at once, and as in flight until its INSERT commits);</li>
 *   <li>a task that goes back to waiting (retry, write-back, lease recovery) is added at once;</li>
 *   <li>a claim is not subtracted until the next re-sync, which only over-counts;</li>
 *   <li>a re-sync replaces the estimate with a database count, plus the reservations still in
 *       flight, plus whatever committed or went back to waiting while the count ran (it may
 *       already be in the count; counting it twice only over-counts).</li>
 * </ul>
 * These steps share one small lock, so a re-sync can never overwrite a reservation made while it
 * ran. Retries of tasks that were already accepted are not refused (they are already promised),
 * so they can lift the backlog above the cap; they then make new submissions be refused sooner.
 */
@Component
public class AdmissionController {

    private final TaskQueueProperties.Admission config;
    private final DoubleSupplier throughput;
    private final Object lock = new Object();
    private long backlog;
    /** Reservations whose INSERT has not committed yet: a database count cannot see them. */
    private long inFlight;
    /** Commits and returns to waiting since the current re-sync began its count. */
    private long changedDuringCount;
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
        synchronized (lock) {
            if (backlog >= threshold) {
                return Decision.overloaded(retryAfterSeconds(backlog - threshold + 1), backlog, threshold);
            }
            backlog++;
            inFlight++;
            return Decision.admit();
        }
    }

    /** The INSERT for a slot reserved by {@link #tryAdmit} has committed. */
    public void confirm() {
        synchronized (lock) {
            inFlight--;
            changedDuringCount++;
        }
    }

    /** Gives back a slot reserved by {@link #tryAdmit} that did not turn into a new task (duplicate, error). */
    public void release() {
        synchronized (lock) {
            inFlight--;
            backlog = Math.max(0, backlog - 1);
        }
    }

    /** Tasks that were running went back to waiting (retry, shutdown write-back, lease recovery, redrive). */
    public void returnedToBacklog(int count) {
        if (count <= 0) {
            return;
        }
        synchronized (lock) {
            backlog += count;
            changedDuringCount += count;
        }
    }

    /** Replaces the estimate with a fresh database count, keeping whatever that count cannot have seen. */
    public void resync(LongSupplier persistedBacklog) {
        synchronized (lock) {
            changedDuringCount = 0;
        }
        long counted = persistedBacklog.getAsLong();
        synchronized (lock) {
            backlog = counted + inFlight + changedDuringCount;
        }
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
        synchronized (lock) {
            return backlog;
        }
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
