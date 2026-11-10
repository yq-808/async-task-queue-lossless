package io.github.yq808.taskqueue.persistence;

import com.google.common.collect.Lists;
import io.github.yq808.taskqueue.domain.ErrorType;
import io.github.yq808.taskqueue.domain.TaskStatus;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * All task state transitions, each in its own short transaction. A transition that also writes a
 * result or an attempt row does so in the same transaction, so the database never shows a
 * SUCCEEDED task without its result.
 */
@Service
public class TaskStore {

    private static final int MAX_ERROR_LENGTH = 1900;
    private static final int IN_CHUNK = 500;

    private final TaskRepository tasks;
    private final TaskResultRepository results;
    private final TaskAttemptRepository attempts;
    private final EntityManager em;

    public TaskStore(TaskRepository tasks, TaskResultRepository results, TaskAttemptRepository attempts,
                     EntityManager em) {
        this.tasks = tasks;
        this.results = results;
        this.attempts = attempts;
        this.em = em;
    }

    /**
     * The persistence boundary of "accepted": once this commits, the task survives a process
     * crash. Uses persist (not save) because the id is assigned, and save would SELECT first.
     */
    @Transactional
    public void insert(TaskEntity task) {
        em.persist(task);
        em.flush();
    }

    @Transactional(readOnly = true)
    public Optional<TaskEntity> find(String id) {
        return tasks.findById(id);
    }

    @Transactional(readOnly = true)
    public Optional<TaskEntity> findByBizKey(String bizKey) {
        return tasks.findByBizKey(bizKey);
    }

    @Transactional(readOnly = true)
    public Optional<TaskResultEntity> findResult(String taskId) {
        return results.findById(taskId);
    }

    @Transactional(readOnly = true)
    public List<TaskAttemptEntity> attemptHistory(String taskId) {
        return attempts.findByTaskIdOrderByIdAsc(taskId);
    }

    /** PENDING/RETRY_WAIT -> RUNNING. Empty when the task is gone, not due, or another worker got it first. */
    @Transactional
    public Optional<ClaimedTask> claim(String id, String owner, Instant now, Duration lease) {
        TaskEntity t = tasks.findById(id).orElse(null);
        if (t == null || !TaskStatus.WAITING.contains(t.getStatus()) || t.getNextRunAt().isAfter(now)) {
            return Optional.empty();
        }
        long waitMs = Math.max(0, now.toEpochMilli() - t.getNextRunAt().toEpochMilli());
        int updated = tasks.claim(id, t.getVersion(), owner, now.plus(lease), waitMs, now,
                TaskStatus.RUNNING, TaskStatus.WAITING);
        if (updated == 0) {
            return Optional.empty();
        }
        return Optional.of(new ClaimedTask(t.getId(), t.getBizKey(), t.getTaskType(), t.getPriority(),
                t.getPayload(), t.getAttempts() + 1, t.getMaxAttempts(), t.getSystemRetries(),
                t.getVersion() + 1, now, waitMs));
    }

    /** RUNNING -> SUCCEEDED plus the result row, atomically. False if the lease was lost. */
    @Transactional
    public boolean complete(ClaimedTask task, String resultJson, long processMs, String worker, Instant now) {
        int updated = tasks.markSucceeded(task.id(), task.version(), processMs, now,
                TaskStatus.SUCCEEDED, TaskStatus.RUNNING);
        if (updated == 0) {
            return false;
        }
        if (!results.existsById(task.id())) {
            em.persist(new TaskResultEntity(task.id(), resultJson, now));
        }
        recordAttempt(task, "SUCCEEDED", null, null, worker, now);
        return true;
    }

    /**
     * RUNNING -> RETRY_WAIT or PENDING. {@code refundAttempt} gives the attempt back when the
     * task itself was not at fault (system fault, shutdown).
     */
    @Transactional
    public boolean reschedule(ClaimedTask task, TaskStatus next, Instant nextRunAt, boolean refundAttempt,
                              boolean systemRetry, ErrorType errorType, String error, long processMs,
                              String worker, Instant now) {
        int updated = tasks.markWaiting(task.id(), task.version(), next, nextRunAt, refundAttempt ? 1 : 0,
                systemRetry ? 1 : 0, truncate(error), errorType, processMs, now, TaskStatus.RUNNING);
        if (updated == 0) {
            return false;
        }
        recordAttempt(task, refundAttempt && errorType == ErrorType.SHUTDOWN_INTERRUPTED ? "RELEASED" : "RETRY",
                errorType, error, worker, now);
        return true;
    }

    /** RUNNING -> FAILED or DEAD_LETTER. */
    @Transactional
    public boolean fail(ClaimedTask task, TaskStatus terminal, ErrorType errorType, String error, long processMs,
                        String worker, Instant now) {
        int updated = tasks.markTerminalFailure(task.id(), task.version(), terminal, truncate(error), errorType,
                processMs, now, TaskStatus.RUNNING);
        if (updated == 0) {
            return false;
        }
        recordAttempt(task, terminal.name(), errorType, error, worker, now);
        return true;
    }

    /**
     * Returns RUNNING rows whose lease ended before {@code expiredBefore} to PENDING, or to
     * DEAD_LETTER when they have no attempts left (a task that keeps killing its worker).
     */
    @Transactional
    public RecoveryResult recoverExpiredLeases(Instant expiredBefore, Instant now) {
        int dead = tasks.deadLetterExpiredLeases(expiredBefore, now,
                "worker lost while running and no attempts left (possible poison task)", ErrorType.LEASE_EXPIRED,
                TaskStatus.DEAD_LETTER, TaskStatus.RUNNING);
        int requeued = tasks.requeueExpiredLeases(expiredBefore, now,
                "worker lost while running (crash or lease expiry); requeued", ErrorType.LEASE_EXPIRED,
                TaskStatus.PENDING, TaskStatus.RUNNING);
        return new RecoveryResult(requeued, dead);
    }

    @Transactional
    public boolean redrive(String id, Instant now) {
        return tasks.redrive(id, now, TaskStatus.PENDING, EnumSet.of(TaskStatus.FAILED, TaskStatus.DEAD_LETTER)) == 1;
    }

    @Transactional(readOnly = true)
    public List<DispatchCandidate> findDispatchable(Instant now, int limit) {
        return tasks.findDispatchable(TaskStatus.WAITING, now, PageRequest.of(0, Math.max(1, limit)));
    }

    /** Due tasks below HIGH priority that have been runnable since before {@code readyBefore}, oldest first. */
    @Transactional(readOnly = true)
    public List<DispatchCandidate> findStarved(Instant readyBefore, int limit) {
        return tasks.findStarved(TaskStatus.WAITING, readyBefore, PageRequest.of(0, Math.max(1, limit)));
    }

    @Transactional(readOnly = true)
    public long countBacklog() {
        return tasks.countByStatusIn(TaskStatus.WAITING);
    }

    /** How many of {@code ids} are still waiting in the database, i.e. safely persisted for a restart. */
    @Transactional(readOnly = true)
    public long countWaiting(Collection<String> ids) {
        long n = 0;
        for (List<String> chunk : Lists.partition(List.copyOf(ids), IN_CHUNK)) {
            n += tasks.countByIdInAndStatusIn(chunk, TaskStatus.WAITING);
        }
        return n;
    }

    @Transactional(readOnly = true)
    public Map<TaskStatus, Long> countByStatus() {
        Map<TaskStatus, Long> counts = new EnumMap<>(TaskStatus.class);
        for (TaskStatus s : TaskStatus.values()) {
            counts.put(s, 0L);
        }
        for (StatusCount c : tasks.countGroupByStatus()) {
            counts.put(c.status(), c.count());
        }
        return counts;
    }

    @Transactional(readOnly = true)
    public Page<TaskEntity> list(TaskStatus status, int page, int size) {
        PageRequest req = PageRequest.of(page, size, Sort.by("createdAt").descending());
        return status == null ? tasks.findAll(req) : tasks.findByStatus(status, req);
    }

    /** Deletes up to {@code batch} SUCCEEDED tasks finished before {@code before}, with their results and history. */
    @Transactional
    public int purgeSucceededBefore(Instant before, int batch) {
        List<String> ids = tasks.findFinishedBefore(TaskStatus.SUCCEEDED, before, PageRequest.of(0, batch));
        if (ids.isEmpty()) {
            return 0;
        }
        results.deleteByTaskIds(ids);
        attempts.deleteByTaskIds(ids);
        return tasks.deleteByIds(ids);
    }

    private void recordAttempt(ClaimedTask task, String outcome, ErrorType errorType, String error, String worker,
                               Instant now) {
        em.persist(new TaskAttemptEntity(task.id(), task.attemptNo(), outcome, errorType, truncate(error), worker,
                task.claimedAt(), now));
    }

    private static String truncate(String s) {
        if (s == null || s.length() <= MAX_ERROR_LENGTH) {
            return s;
        }
        return s.substring(0, MAX_ERROR_LENGTH) + "…";
    }

    public record RecoveryResult(int requeued, int deadLettered) {

        public int total() {
            return requeued + deadLettered;
        }
    }
}
