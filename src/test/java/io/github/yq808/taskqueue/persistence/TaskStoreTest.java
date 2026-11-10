package io.github.yq808.taskqueue.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.yq808.taskqueue.domain.ErrorType;
import io.github.yq808.taskqueue.domain.TaskStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;

/** State transitions in isolation: exclusivity of claims and fencing of stale workers. */
@DataJpaTest(properties = {"spring.jpa.hibernate.ddl-auto=none", "spring.sql.init.mode=always"})
@Import(TaskStore.class)
class TaskStoreTest {

    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final Instant FAR_FUTURE = Instant.parse("9999-12-31T00:00:00Z");

    @Autowired
    private TaskStore store;
    @Autowired
    private TaskResultRepository results;
    @Autowired
    private TaskAttemptRepository attempts;

    private final Instant now = Instant.parse("2026-11-10T00:00:00Z");

    private String newTask() {
        String id = UUID.randomUUID().toString();
        store.insert(TaskEntity.newPending(id, "key-" + id, "SUMMARIZE", 1, "{\"text\":\"x\"}", "h".repeat(64), 3, now));
        return id;
    }

    @Test
    void onlyOneClaimWins() {
        String id = newTask();

        assertThat(store.claim(id, "a", now, LEASE)).isPresent();
        assertThat(store.claim(id, "b", now, LEASE)).isEmpty();
        assertThat(store.find(id).orElseThrow().getStatus()).isEqualTo(TaskStatus.RUNNING);
    }

    @Test
    void aWorkerThatLostItsLeaseCannotWriteItsOutcome() {
        String id = newTask();
        ClaimedTask stale = store.claim(id, "slow-worker", now, LEASE).orElseThrow();

        // the lease runs out, the reaper hands the task to someone else
        assertThat(store.recoverExpiredLeases(FAR_FUTURE, now).requeued()).isEqualTo(1);
        ClaimedTask fresh = store.claim(id, "new-worker", now, LEASE).orElseThrow();
        assertThat(fresh.version()).isGreaterThan(stale.version());
        assertThat(fresh.attemptNo()).isEqualTo(2);

        assertThat(store.complete(stale, "{\"from\":\"stale\"}", 10, "slow-worker", now)).isFalse();
        assertThat(store.complete(fresh, "{\"from\":\"fresh\"}", 10, "new-worker", now)).isTrue();
        assertThat(results.findById(id).orElseThrow().getResult()).isEqualTo("{\"from\":\"fresh\"}");
        assertThat(store.fail(stale, TaskStatus.DEAD_LETTER, ErrorType.SYSTEM_FATAL, "late", 1, "slow-worker", now))
                .isFalse();
        assertThat(store.find(id).orElseThrow().getStatus()).isEqualTo(TaskStatus.SUCCEEDED);
    }

    @Test
    void completionWritesStatusResultAndHistoryTogether() {
        String id = newTask();
        ClaimedTask c = store.claim(id, "w", now, LEASE).orElseThrow();

        assertThat(store.complete(c, "{\"ok\":true}", 42, "w", now.plusMillis(42))).isTrue();

        TaskEntity t = store.find(id).orElseThrow();
        assertThat(t.getStatus()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(t.getProcessMs()).isEqualTo(42);
        assertThat(t.getLeaseOwner()).isNull();
        assertThat(results.existsById(id)).isTrue();
        assertThat(attempts.findByTaskIdOrderByIdAsc(id)).extracting(TaskAttemptEntity::getOutcome)
                .containsExactly("SUCCEEDED");
    }

    @Test
    void aRetryIsNotClaimableBeforeItsBackoffEnds() {
        String id = newTask();
        ClaimedTask c = store.claim(id, "w", now, LEASE).orElseThrow();
        store.reschedule(c, TaskStatus.RETRY_WAIT, now.plusSeconds(10), false, false, ErrorType.BUSINESS_RETRYABLE,
                "429", 5, "w", now);

        assertThat(store.findDispatchable(now.plusSeconds(5), 10)).isEmpty();
        assertThat(store.claim(id, "w", now.plusSeconds(5), LEASE)).isEmpty();
        assertThat(store.findDispatchable(now.plusSeconds(11), 10)).extracting(DispatchCandidate::id).containsExactly(id);
        assertThat(store.claim(id, "w", now.plusSeconds(11), LEASE)).map(ClaimedTask::attemptNo).contains(2);
    }

    @Test
    void refundedAttemptsDoNotCountAgainstTheBudget() {
        String id = newTask();
        ClaimedTask c = store.claim(id, "w", now, LEASE).orElseThrow();
        store.reschedule(c, TaskStatus.PENDING, now, true, false, ErrorType.SHUTDOWN_INTERRUPTED, "stopping", 5, "w",
                now);

        TaskEntity t = store.find(id).orElseThrow();
        assertThat(t.getAttempts()).isZero();
        assertThat(t.getStatus()).isEqualTo(TaskStatus.PENDING);
        assertThat(attempts.findByTaskIdOrderByIdAsc(id)).extracting(TaskAttemptEntity::getOutcome)
                .containsExactly("RELEASED");
    }

    @Test
    void anOrphanWithNoAttemptsLeftIsDeadLettered() {
        String id = UUID.randomUUID().toString();
        store.insert(TaskEntity.newPending(id, "key-" + id, "SUMMARIZE", 1, "{}", "h".repeat(64), 1, now));
        store.claim(id, "w", now, LEASE).orElseThrow();

        TaskStore.RecoveryResult r = store.recoverExpiredLeases(FAR_FUTURE, now);

        assertThat(r.deadLettered()).isEqualTo(1);
        TaskEntity t = store.find(id).orElseThrow();
        assertThat(t.getStatus()).isEqualTo(TaskStatus.DEAD_LETTER);
        assertThat(t.getErrorType()).isEqualTo(ErrorType.LEASE_EXPIRED);
    }

    @Test
    void dispatchOrderIsPriorityThenAge() {
        String low = UUID.randomUUID().toString();
        String high = UUID.randomUUID().toString();
        String normal = UUID.randomUUID().toString();
        store.insert(TaskEntity.newPending(low, "k-" + low, "SUMMARIZE", 2, "{}", "h".repeat(64), 3, now));
        store.insert(TaskEntity.newPending(high, "k-" + high, "SUMMARIZE", 0, "{}", "h".repeat(64), 3, now.plusSeconds(2)));
        store.insert(TaskEntity.newPending(normal, "k-" + normal, "SUMMARIZE", 1, "{}", "h".repeat(64), 3, now.plusSeconds(1)));

        assertThat(store.findDispatchable(now.plusSeconds(5), 10)).extracting(DispatchCandidate::id)
                .containsExactly(high, normal, low);
    }

    @Test
    void purgeRemovesOldSucceededTasksWithTheirResults() {
        String id = newTask();
        ClaimedTask c = store.claim(id, "w", now, LEASE).orElseThrow();
        store.complete(c, "{}", 1, "w", now);
        String pending = newTask();

        assertThat(store.purgeSucceededBefore(now.plusSeconds(1), 100)).isEqualTo(1);

        assertThat(store.find(id)).isEmpty();
        assertThat(results.existsById(id)).isFalse();
        assertThat(attempts.findByTaskIdOrderByIdAsc(id)).isEmpty();
        assertThat(store.find(pending)).isPresent();
        assertThat(store.countWaiting(List.of(id, pending))).isEqualTo(1);
    }
}
