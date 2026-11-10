package io.github.yq808.taskqueue.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.yq808.taskqueue.config.TaskQueueProperties;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

    private static TaskQueueProperties.Retry config(double jitter) {
        return new TaskQueueProperties.Retry(4, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10), jitter, 10);
    }

    @Test
    void growsExponentiallyUpToTheCap() {
        RetryPolicy policy = new RetryPolicy(config(0), () -> 0.5);

        assertThat(policy.backoff(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.backoff(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.backoff(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(policy.backoff(4)).isEqualTo(Duration.ofSeconds(8));
        assertThat(policy.backoff(5)).isEqualTo(Duration.ofSeconds(10));
        assertThat(policy.backoff(30)).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void jitterStaysWithinItsBand() {
        assertThat(new RetryPolicy(config(0.2), () -> 0.0).backoff(2)).isEqualTo(Duration.ofMillis(1600));
        assertThat(new RetryPolicy(config(0.2), () -> 1.0).backoff(2)).isEqualTo(Duration.ofMillis(2400));
        assertThat(new RetryPolicy(config(0.2), () -> 0.5).backoff(2)).isEqualTo(Duration.ofMillis(2000));
    }
}
