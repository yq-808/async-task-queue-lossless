package io.github.yq808.taskqueue.engine;

import io.github.yq808.taskqueue.config.TaskQueueProperties;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Exponential backoff with jitter: {@code initial * multiplier^(n-1)}, capped at {@code max},
 * then spread by +/- {@code jitter}. A retry waits on disk (RETRY_WAIT with a future
 * next_run_at), not in the memory queue, so a burst of failures cannot crowd out new work.
 */
@Component
public class RetryPolicy {

    private final TaskQueueProperties.Retry config;
    private final DoubleSupplier random;

    @Autowired
    public RetryPolicy(TaskQueueProperties properties) {
        this(properties.retry(), () -> ThreadLocalRandom.current().nextDouble());
    }

    RetryPolicy(TaskQueueProperties.Retry config, DoubleSupplier random) {
        this.config = config;
        this.random = random;
    }

    /** Delay before the run that follows failure number {@code failures} (1-based). */
    public Duration backoff(int failures) {
        double base = config.initialBackoff().toMillis() * Math.pow(config.multiplier(), Math.max(0, failures - 1));
        double capped = Math.min(base, config.maxBackoff().toMillis());
        double spread = 1 + config.jitter() * (2 * random.getAsDouble() - 1);
        return Duration.ofMillis(Math.max(0, Math.round(capped * spread)));
    }

    public int maxSystemRetries() {
        return config.maxSystemRetries();
    }
}
