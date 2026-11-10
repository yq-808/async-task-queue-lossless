package io.github.yq808.taskqueue.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.yq808.taskqueue.TaskQueueApplication;
import io.github.yq808.taskqueue.domain.ErrorType;
import io.github.yq808.taskqueue.domain.TaskStatus;
import io.github.yq808.taskqueue.metrics.TaskMetrics;
import io.github.yq808.taskqueue.persistence.TaskEntity;
import io.github.yq808.taskqueue.persistence.TaskStore;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * What a crashed process leaves behind is RUNNING rows that nobody owns. These tests write such
 * rows directly, then check that startup recovery and the lease reaper bring them back.
 * (The real kill -9 run is scripts/crash-test.sh.)
 */
class CrashRecoveryIntegrationTest {

    private final String dataDir = TestDataDir.create("crash");

    private ConfigurableApplicationContext start() {
        return new SpringApplicationBuilder(TaskQueueApplication.class)
                .web(WebApplicationType.NONE)
                .profiles("it")
                .run("--taskqueue.data-dir=" + dataDir);
    }

    private static void insertRunning(JdbcTemplate jdbc, String id, int attempts, int maxAttempts, Instant leaseUntil) {
        Timestamp now = Timestamp.from(Instant.now());
        jdbc.update("INSERT INTO task (id, biz_key, task_type, priority, status, payload, payload_hash, attempts, "
                        + "max_attempts, next_run_at, lease_owner, lease_until, version, created_at, started_at, updated_at) "
                        + "VALUES (?, ?, 'SUMMARIZE', 1, 'RUNNING', '{\"text\":\"orphan\"}', ?, ?, ?, ?, 'dead-process', ?, 1, ?, ?, ?)",
                id, "orphan-" + id, "0".repeat(64), attempts, maxAttempts, now, Timestamp.from(leaseUntil), now, now, now);
    }

    @Test
    void startupRecoversTasksLeftRunningByADeadProcess() {
        List<String> orphans = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                UUID.randomUUID().toString());
        String poison = UUID.randomUUID().toString();

        try (ConfigurableApplicationContext first = start()) {
            JdbcTemplate jdbc = first.getBean(JdbcTemplate.class);
            Instant farLease = Instant.now().plus(Duration.ofDays(1)); // the reaper would not touch these yet
            orphans.forEach(id -> insertRunning(jdbc, id, 1, 4, farLease));
            insertRunning(jdbc, poison, 4, 4, farLease);
        }

        try (ConfigurableApplicationContext second = start()) {
            TaskStore store = second.getBean(TaskStore.class);
            await().atMost(Duration.ofSeconds(10)).until(() ->
                    orphans.stream().allMatch(id -> store.find(id).orElseThrow().getStatus() == TaskStatus.SUCCEEDED));

            TaskEntity recovered = store.find(orphans.get(0)).orElseThrow();
            assertThat(recovered.getAttempts()).as("the lost attempt counts, then one more run").isEqualTo(2);
            TaskEntity dead = store.find(poison).orElseThrow();
            assertThat(dead.getStatus()).isEqualTo(TaskStatus.DEAD_LETTER);
            assertThat(dead.getErrorType()).isEqualTo(ErrorType.LEASE_EXPIRED);
            assertThat(second.getBean(TaskMetrics.class).recovered.count()).isEqualTo(4.0);
        }
    }

    @Test
    void leaseReaperRequeuesATaskWhoseWorkerVanished() {
        try (ConfigurableApplicationContext ctx = start()) {
            JdbcTemplate jdbc = ctx.getBean(JdbcTemplate.class);
            String id = UUID.randomUUID().toString();
            insertRunning(jdbc, id, 1, 4, Instant.now().minusSeconds(1));

            TaskStore store = ctx.getBean(TaskStore.class);
            await().atMost(Duration.ofSeconds(10))
                    .until(() -> store.find(id).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
            assertThat(ctx.getBean(TaskMetrics.class).recovered.count()).isEqualTo(1.0);
        }
    }
}
