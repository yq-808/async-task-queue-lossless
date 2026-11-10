package io.github.yq808.taskqueue.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.yq808.taskqueue.metrics.TaskMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class DownstreamCircuitBreakerTest {

    private final AtomicLong clock = new AtomicLong();
    private final TaskMetrics metrics = new TaskMetrics(new SimpleMeterRegistry());
    private final DownstreamCircuitBreaker breaker =
            new DownstreamCircuitBreaker(3, TimeUnit.SECONDS.toNanos(5), clock::get, metrics);

    @Test
    void opensAfterConsecutiveSystemFailuresAndClosesAfterTheOpenPeriod() {
        breaker.onSystemFailure();
        breaker.onSystemFailure();
        assertThat(breaker.isOpen()).isFalse();
        breaker.onSystemFailure();
        assertThat(breaker.isOpen()).isTrue();
        assertThat(metrics.breakerOpened.count()).isEqualTo(1.0);

        clock.addAndGet(TimeUnit.SECONDS.toNanos(4));
        assertThat(breaker.remainingOpenNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(1));
        clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
        assertThat(breaker.isOpen()).isFalse();
    }

    @Test
    void aSuccessResetsTheCount() {
        breaker.onSystemFailure();
        breaker.onSystemFailure();
        breaker.onSuccess();
        breaker.onSystemFailure();
        breaker.onSystemFailure();
        assertThat(breaker.isOpen()).isFalse();
    }
}
