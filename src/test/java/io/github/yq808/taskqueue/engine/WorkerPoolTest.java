package io.github.yq808.taskqueue.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.yq808.taskqueue.metrics.TaskMetrics;
import io.github.yq808.taskqueue.persistence.ClaimedTask;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class WorkerPoolTest {

    @Test
    void aWorkerThatOutlivedAStopExitsInsteadOfJoiningTheRestartedPool() throws Exception {
        TaskDispatcher dispatcher = mock(TaskDispatcher.class);
        TaskProcessor processor = mock(TaskProcessor.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DownstreamCircuitBreaker breaker = new DownstreamCircuitBreaker(
                TestProperties.with(1, 8, Duration.ofSeconds(30)), new TaskMetrics(registry));
        WorkerPool pool = new WorkerPool(TestProperties.with(1, 8, Duration.ofSeconds(30)), dispatcher, processor,
                breaker, registry);

        QueuedTask task = new QueuedTask("stuck", 1);
        when(dispatcher.poll(anyLong(), any())).thenReturn(task).thenAnswer(inv -> {
            Thread.sleep(5);
            return null;
        });
        when(processor.claim(any(), any())).thenReturn(Optional.of(new ClaimedTask("stuck", "k", "SUMMARIZE", 1,
                "{}", 1, 4, 0, 1, Instant.now(), 0)));
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Thread> stuckThread = new AtomicReference<>();
        doAnswer(inv -> {
            stuckThread.set(Thread.currentThread());
            while (true) { // ignores interrupts, like a call stuck in native code
                try {
                    if (release.await(10, TimeUnit.SECONDS)) {
                        return null;
                    }
                } catch (InterruptedException ignored) {
                    // keep waiting
                }
            }
        }).when(processor).execute(any(), any());

        pool.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> stuckThread.get() != null);
        WorkerPool.StopReport report = pool.stop(Duration.ofMillis(50), Duration.ofMillis(50));
        assertThat(report.stillAlive()).isEqualTo(1);

        pool.start(); // a restart in the same application
        release.countDown();

        await().atMost(Duration.ofSeconds(5)).until(() -> !stuckThread.get().isAlive());
        pool.stop(Duration.ofSeconds(1), Duration.ofSeconds(1));
    }
}
