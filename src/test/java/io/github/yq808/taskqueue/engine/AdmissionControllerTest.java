package io.github.yq808.taskqueue.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.yq808.taskqueue.config.TaskQueueProperties;
import io.github.yq808.taskqueue.domain.TaskPriority;
import io.github.yq808.taskqueue.engine.AdmissionController.Decision.Outcome;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AdmissionControllerTest {

    private final AtomicReference<Double> throughput = new AtomicReference<>(0.0);

    private AdmissionController controller(boolean adaptive) {
        var config = new TaskQueueProperties.Admission(100, adaptive, 20, Duration.ofSeconds(10), 0.9, 0.7);
        AdmissionController c = new AdmissionController(config, throughput::get);
        c.open();
        return c;
    }

    @Test
    void refusesEverythingUntilOpenedAndAfterClosed() {
        var c = new AdmissionController(new TaskQueueProperties.Admission(100, false, 20, Duration.ofSeconds(10),
                0.9, 0.7), () -> 0.0);
        assertThat(c.tryAdmit(TaskPriority.HIGH).outcome()).isEqualTo(Outcome.SHUTTING_DOWN);
        c.open();
        assertThat(c.tryAdmit(TaskPriority.HIGH).admitted()).isTrue();
        c.close();
        assertThat(c.tryAdmit(TaskPriority.HIGH).outcome()).isEqualTo(Outcome.SHUTTING_DOWN);
    }

    @Test
    void shedsLowPriorityFirst() {
        var c = controller(false);
        c.resync(70);
        assertThat(c.tryAdmit(TaskPriority.LOW).outcome()).isEqualTo(Outcome.OVERLOADED);
        assertThat(c.tryAdmit(TaskPriority.NORMAL).admitted()).isTrue();
        c.resync(90);
        assertThat(c.tryAdmit(TaskPriority.NORMAL).outcome()).isEqualTo(Outcome.OVERLOADED);
        assertThat(c.tryAdmit(TaskPriority.HIGH).admitted()).isTrue();
        c.resync(100);
        assertThat(c.tryAdmit(TaskPriority.HIGH).outcome()).isEqualTo(Outcome.OVERLOADED);
    }

    @Test
    void adaptiveLimitFollowsThroughputWithinBounds() {
        var c = controller(true);
        assertThat(c.currentLimit()).as("no throughput sample yet: hard cap").isEqualTo(100);
        throughput.set(5.0);
        assertThat(c.currentLimit()).as("5/s * 10s").isEqualTo(50);
        throughput.set(0.5);
        assertThat(c.currentLimit()).as("floor").isEqualTo(20);
        throughput.set(1000.0);
        assertThat(c.currentLimit()).as("ceiling").isEqualTo(100);
    }

    @Test
    void retryAfterIsTheTimeToDrainTheExcess() {
        var c = controller(false);
        throughput.set(2.0);
        c.resync(100);
        var d = c.tryAdmit(TaskPriority.HIGH);
        assertThat(d.outcome()).isEqualTo(Outcome.OVERLOADED);
        assertThat(d.retryAfterSeconds()).isEqualTo(1);
        c.resync(140);
        assertThat(c.tryAdmit(TaskPriority.HIGH).retryAfterSeconds()).isEqualTo(21);
    }

    @Test
    void concurrentAdmissionsNeverOvershootTheCap() throws Exception {
        var c = controller(false);
        AtomicInteger admitted = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < 1000; i++) {
            pool.submit(() -> {
                start.await();
                if (c.tryAdmit(TaskPriority.HIGH).admitted()) {
                    admitted.incrementAndGet();
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        assertThat(admitted.get()).isEqualTo(100);
        assertThat(c.backlog()).isEqualTo(100);
    }

    @Test
    void releaseGivesTheSlotBack() {
        var c = controller(false);
        c.resync(99);
        assertThat(c.tryAdmit(TaskPriority.HIGH).admitted()).isTrue();
        assertThat(c.tryAdmit(TaskPriority.HIGH).admitted()).isFalse();
        c.release();
        assertThat(c.tryAdmit(TaskPriority.HIGH).admitted()).isTrue();
    }
}
