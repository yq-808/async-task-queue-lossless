package io.github.yq808.taskqueue.engine;

import io.github.yq808.taskqueue.config.TaskQueueProperties;
import java.time.Duration;

/** Default settings for unit tests that build engine components by hand. */
final class TestProperties {

    private TestProperties() {
    }

    static TaskQueueProperties with(int workers, int queueCapacity, Duration agingThreshold) {
        return new TaskQueueProperties(
                new TaskQueueProperties.Worker(workers),
                new TaskQueueProperties.Queue(queueCapacity, 8, 0.5, agingThreshold),
                new TaskQueueProperties.Admission(100, false, 10, Duration.ofMinutes(10), 0.9, 0.7),
                new TaskQueueProperties.Retry(4, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(60), 0.2, 10),
                new TaskQueueProperties.Execution(Duration.ofSeconds(10), Duration.ofSeconds(60),
                        Duration.ofSeconds(10), 5, Duration.ofSeconds(5)),
                new TaskQueueProperties.Dispatcher(Duration.ofMillis(200)),
                new TaskQueueProperties.Shutdown(Duration.ofSeconds(20), Duration.ofSeconds(5)),
                new TaskQueueProperties.Retention(Duration.ofDays(7), Duration.ofHours(1)),
                new TaskQueueProperties.Simulation(Duration.ofMillis(200), Duration.ofMillis(1500), 0, 0, 1));
    }
}
