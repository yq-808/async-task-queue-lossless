package io.github.yq808.taskqueue.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yq808.taskqueue.TaskQueueApplication;
import io.github.yq808.taskqueue.domain.TaskPriority;
import io.github.yq808.taskqueue.domain.TaskStatus;
import io.github.yq808.taskqueue.engine.TaskEngine;
import io.github.yq808.taskqueue.engine.TaskSubmissionService;
import io.github.yq808.taskqueue.engine.TaskSubmissionService.SubmitCommand;
import io.github.yq808.taskqueue.engine.TaskSubmissionService.SubmitResult;
import io.github.yq808.taskqueue.handler.SummarizeTaskHandler;
import io.github.yq808.taskqueue.persistence.TaskAttemptEntity;
import io.github.yq808.taskqueue.persistence.TaskEntity;
import io.github.yq808.taskqueue.persistence.TaskStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Stop the application while tasks are running and queued, start it again on the same H2 files,
 * and check that every accepted task completes, and that the attempt interrupted by the stop was
 * handed back rather than counted against the task.
 */
class GracefulShutdownRestartTest {

    private final String dataDir = TestDataDir.create("shutdown");

    private ConfigurableApplicationContext start(String latency, String awaitRunning) {
        return new SpringApplicationBuilder(TaskQueueApplication.class)
                .web(WebApplicationType.NONE)
                .profiles("it")
                .run("--taskqueue.data-dir=" + dataDir,
                        "--taskqueue.worker.threads=2",
                        "--taskqueue.execution.timeout=20s",
                        "--taskqueue.execution.lease=60s",
                        "--taskqueue.simulation.min-latency=" + latency,
                        "--taskqueue.simulation.max-latency=" + latency,
                        "--taskqueue.shutdown.await-running=" + awaitRunning,
                        "--taskqueue.shutdown.await-interrupted=5s");
    }

    @Test
    void stopWritesBackAndRestartFinishesEverything() {
        List<String> ids = new ArrayList<>();
        TaskEngine.ShutdownReport report;

        // Run 1: tasks take 10 s, the stop allows 300 ms, so the 2 running ones must be interrupted.
        try (ConfigurableApplicationContext first = start("10s", "300ms")) {
            TaskSubmissionService submissions = first.getBean(TaskSubmissionService.class);
            ObjectMapper mapper = first.getBean(ObjectMapper.class);
            for (int i = 0; i < 10; i++) {
                SubmitResult r = submissions.submit(new SubmitCommand("stop-" + i, SummarizeTaskHandler.TYPE,
                        TaskPriority.NORMAL, mapper.valueToTree(Map.of("text", "task " + i)), null));
                ids.add(((SubmitResult.Accepted) r).task().getId());
            }
            TaskStore store = first.getBean(TaskStore.class);
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> store.countByStatus().get(TaskStatus.RUNNING) == 2);
            TaskEngine engine = first.getBean(TaskEngine.class);
            long stopStarted = System.nanoTime();
            first.close();
            long stopMillis = (System.nanoTime() - stopStarted) / 1_000_000;
            report = engine.lastShutdown();
            assertThat(stopMillis).as("the pool converges within the configured limits").isLessThan(6_000);
        }

        assertThat(report.interruptedRunning()).isEqualTo(2);
        assertThat(report.drainedFromMemory()).isEqualTo(8);
        assertThat(report.confirmedWaiting()).as("every drained task is safe in H2").isEqualTo(8);
        assertThat(report.workersStillAlive()).isZero();

        // Run 2: same files, fast tasks. Everything accepted in run 1 must finish.
        try (ConfigurableApplicationContext second = start("10ms", "2s")) {
            TaskStore store = second.getBean(TaskStore.class);
            await().atMost(Duration.ofSeconds(10))
                    .until(() -> ids.stream().allMatch(id -> store.find(id).orElseThrow().getStatus() == TaskStatus.SUCCEEDED));

            assertThat(second.getBean(SummarizeTaskHandler.class).invocations()).isEqualTo(10);
            int released = 0;
            for (String id : ids) {
                TaskEntity task = store.find(id).orElseThrow();
                assertThat(task.getAttempts()).as("the interrupted attempt was refunded").isEqualTo(1);
                List<String> outcomes = store.attemptHistory(id).stream().map(TaskAttemptEntity::getOutcome).toList();
                if (outcomes.size() == 2) {
                    assertThat(outcomes).containsExactly("RELEASED", "SUCCEEDED");
                    released++;
                } else {
                    assertThat(outcomes).containsExactly("SUCCEEDED");
                }
            }
            assertThat(released).isEqualTo(2);
        }
    }
}
