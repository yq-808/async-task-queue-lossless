package io.github.yq808.taskqueue.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yq808.taskqueue.TaskQueueApplication;
import io.github.yq808.taskqueue.domain.TaskPriority;
import io.github.yq808.taskqueue.domain.TaskStatus;
import io.github.yq808.taskqueue.engine.TaskSubmissionService;
import io.github.yq808.taskqueue.engine.TaskSubmissionService.SubmitCommand;
import io.github.yq808.taskqueue.engine.TaskSubmissionService.SubmitResult;
import io.github.yq808.taskqueue.engine.WorkerPool;
import io.github.yq808.taskqueue.handler.SummarizeTaskHandler;
import io.github.yq808.taskqueue.persistence.TaskStore;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** stop() then start() on the same application context brings the whole pipeline back. */
class EngineRestartIntegrationTest {

    @Test
    void theEngineRunsTasksAgainAfterAStopAndStartInTheSameContext() {
        try (ConfigurableApplicationContext ctx = new SpringApplicationBuilder(TaskQueueApplication.class)
                .web(WebApplicationType.NONE)
                .profiles("it")
                .run("--taskqueue.data-dir=" + TestDataDir.create("restart"))) {
            TaskSubmissionService submissions = ctx.getBean(TaskSubmissionService.class);
            TaskStore store = ctx.getBean(TaskStore.class);
            ObjectMapper mapper = ctx.getBean(ObjectMapper.class);

            String first = submit(submissions, mapper, "restart-1");
            await().atMost(Duration.ofSeconds(10)).until(() -> status(store, first) == TaskStatus.SUCCEEDED);

            ctx.stop();
            SubmitResult refused = submissions.submit(command(mapper, "restart-refused"));
            assertThat(refused).isInstanceOf(SubmitResult.Rejected.class);

            ctx.start();
            String second = submit(submissions, mapper, "restart-2");
            await().atMost(Duration.ofSeconds(10)).until(() -> status(store, second) == TaskStatus.SUCCEEDED);
            assertThat(ctx.getBean(SummarizeTaskHandler.class).invocations()).isEqualTo(2);
            assertThat(ctx.getBean(WorkerPool.class).activeCount()).isZero();
        }
    }

    private static SubmitCommand command(ObjectMapper mapper, String key) {
        return new SubmitCommand(key, SummarizeTaskHandler.TYPE, TaskPriority.NORMAL,
                mapper.valueToTree(Map.of("text", key)), null);
    }

    private static String submit(TaskSubmissionService submissions, ObjectMapper mapper, String key) {
        return ((SubmitResult.Accepted) submissions.submit(command(mapper, key))).task().getId();
    }

    private static TaskStatus status(TaskStore store, String id) {
        return store.find(id).orElseThrow().getStatus();
    }
}
