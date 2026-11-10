package io.github.yq808.taskqueue.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.yq808.taskqueue.config.TaskQueueProperties;
import io.github.yq808.taskqueue.metrics.TaskMetrics;
import io.github.yq808.taskqueue.persistence.DispatchCandidate;
import io.github.yq808.taskqueue.persistence.TaskStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Refill decisions with a stubbed database, so the order of events is fully controlled. */
class TaskDispatcherTest {

    private static final int HIGH = 0;
    private static final int LOW = 2;
    private static final Instant NOW = Instant.parse("2026-11-10T00:00:00Z");

    private final TaskStore store = mock(TaskStore.class);
    private TaskDispatcher dispatcher;

    /** Capacity 4, low watermark 2, aging 30 s. */
    @BeforeEach
    void setUp() {
        TaskQueueProperties props = new TaskQueueProperties(
                new TaskQueueProperties.Worker(1),
                new TaskQueueProperties.Queue(4, 8, 0.5, Duration.ofSeconds(30)),
                new TaskQueueProperties.Admission(100, false, 10, Duration.ofMinutes(10), 0.9, 0.7),
                new TaskQueueProperties.Retry(4, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(60), 0.2, 10),
                new TaskQueueProperties.Execution(Duration.ofSeconds(10), Duration.ofSeconds(60),
                        Duration.ofSeconds(10), 5, Duration.ofSeconds(5)),
                new TaskQueueProperties.Dispatcher(Duration.ofMillis(200)),
                new TaskQueueProperties.Shutdown(Duration.ofSeconds(20), Duration.ofSeconds(5)),
                new TaskQueueProperties.Retention(Duration.ofDays(7), Duration.ofHours(1)),
                new TaskQueueProperties.Simulation(Duration.ofMillis(200), Duration.ofMillis(1500), 0, 0, 1));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        dispatcher = new TaskDispatcher(props, store, Clock.fixed(NOW, ZoneOffset.UTC), new TaskMetrics(registry),
                registry);
        dispatcher.open(); // fast path and refill enabled, no background thread
        when(store.findStarved(any(), anyInt())).thenReturn(List.of());
    }

    private static DispatchCandidate row(String id, int priority) {
        return new DispatchCandidate(id, priority, NOW);
    }

    private List<String> drain() throws InterruptedException {
        List<String> ids = new ArrayList<>();
        QueuedTask t;
        while ((t = dispatcher.poll(1, TimeUnit.MILLISECONDS)) != null) {
            ids.add(t.id());
            dispatcher.forget(t.id());
        }
        return ids;
    }

    @Test
    void aSpilledUrgentTaskDisplacesLessUrgentWorkWithoutWaitingForTheWatermark() throws Exception {
        for (int i = 1; i <= 4; i++) {
            dispatcher.offerNew("high-" + i, HIGH);
        }
        dispatcher.offerNew("high-5", HIGH); // memory full of HIGH: spills to disk
        QueuedTask first = dispatcher.poll(1, TimeUnit.MILLISECONDS);
        dispatcher.forget(first.id());
        dispatcher.offerNew("low-1", LOW);   // takes the free slot; size 4 is above the watermark
        when(store.findDispatchable(any(), anyInt())).thenReturn(List.of(
                row("high-2", HIGH), row("high-3", HIGH), row("high-4", HIGH), row("high-5", HIGH), row("low-1", LOW)));

        assertThat(dispatcher.refill()).isEqualTo(1);

        assertThat(drain()).containsExactly("high-2", "high-3", "high-4", "high-5");
    }

    @Test
    void noRefillAboveTheWatermarkWhenNothingMoreUrgentSpilled() {
        dispatcher.offerNew("low-1", LOW);
        dispatcher.offerNew("low-2", LOW);
        dispatcher.offerNew("low-3", LOW);

        assertThat(dispatcher.refill()).isZero();
    }

    @Test
    void aLowPriorityTaskStarvingOnDiskIsLoadedAheadOfUrgentWork() throws Exception {
        when(store.findStarved(any(), anyInt())).thenReturn(List.of(
                new DispatchCandidate("low-old", LOW, NOW.minusSeconds(45))));
        when(store.findDispatchable(any(), anyInt())).thenReturn(List.of(
                row("high-1", HIGH), row("high-2", HIGH), row("high-3", HIGH), row("high-4", HIGH),
                new DispatchCandidate("low-old", LOW, NOW.minusSeconds(45))));

        assertThat(dispatcher.refill()).isEqualTo(4);

        assertThat(drain()).containsExactly("low-old", "high-1", "high-2", "high-3");
    }

    @Test
    void canBeReopenedAfterCloseAndDrain() {
        dispatcher.offerNew("a", HIGH);
        assertThat(dispatcher.closeAndDrain()).containsExactly("a");
        dispatcher.offerNew("b", HIGH);
        assertThat(dispatcher.size()).isZero();

        dispatcher.open();
        dispatcher.offerNew("b", HIGH);
        assertThat(dispatcher.size()).isEqualTo(1);
    }
}
