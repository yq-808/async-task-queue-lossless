package io.github.yq808.taskqueue.persistence;

import io.github.yq808.taskqueue.domain.ErrorType;
import io.github.yq808.taskqueue.domain.TaskStatus;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Every transition is a conditional update that names the state it expects. A return value of 0
 * means someone else moved the task first (another worker, the lease reaper, a redrive), and the
 * caller must back off instead of overwriting.
 */
public interface TaskRepository extends JpaRepository<TaskEntity, String> {

    Optional<TaskEntity> findByBizKey(String bizKey);

    Page<TaskEntity> findByStatus(TaskStatus status, Pageable pageable);

    long countByStatusIn(Collection<TaskStatus> statuses);

    long countByIdInAndStatusIn(Collection<String> ids, Collection<TaskStatus> statuses);

    @Query("select new io.github.yq808.taskqueue.persistence.StatusCount(t.status, count(t)) "
            + "from TaskEntity t group by t.status")
    List<StatusCount> countGroupByStatus();

    @Query("select new io.github.yq808.taskqueue.persistence.DispatchCandidate(t.id, t.priority, t.nextRunAt) "
            + "from TaskEntity t where t.status in :waiting and t.nextRunAt <= :now "
            + "order by t.priority asc, t.nextRunAt asc")
    List<DispatchCandidate> findDispatchable(@Param("waiting") Collection<TaskStatus> waiting,
                                             @Param("now") Instant now, Pageable limit);

    /** PENDING/RETRY_WAIT -> RUNNING, only if nobody touched the row since it was read. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update TaskEntity t set t.status = :running, t.attempts = t.attempts + 1, t.version = t.version + 1, "
            + "t.leaseOwner = :owner, t.leaseUntil = :leaseUntil, t.queueWaitMs = t.queueWaitMs + :waitMs, "
            + "t.startedAt = coalesce(t.startedAt, :now), t.updatedAt = :now "
            + "where t.id = :id and t.version = :version and t.status in :waiting and t.nextRunAt <= :now")
    int claim(@Param("id") String id, @Param("version") long version, @Param("owner") String owner,
              @Param("leaseUntil") Instant leaseUntil, @Param("waitMs") long waitMs, @Param("now") Instant now,
              @Param("running") TaskStatus running, @Param("waiting") Collection<TaskStatus> waiting);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update TaskEntity t set t.status = :succeeded, t.finishedAt = :now, t.processMs = t.processMs + :processMs, "
            + "t.leaseOwner = null, t.leaseUntil = null, t.updatedAt = :now "
            + "where t.id = :id and t.version = :version and t.status = :running")
    int markSucceeded(@Param("id") String id, @Param("version") long version, @Param("processMs") long processMs,
                      @Param("now") Instant now, @Param("succeeded") TaskStatus succeeded,
                      @Param("running") TaskStatus running);

    /** RUNNING -> RETRY_WAIT (counts the attempt) or PENDING (refunds it), with the next run time. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update TaskEntity t set t.status = :next, t.nextRunAt = :nextRunAt, "
            + "t.attempts = t.attempts - :refundAttempts, t.systemRetries = t.systemRetries + :addSystemRetries, "
            + "t.lastError = :error, t.errorType = :errorType, t.processMs = t.processMs + :processMs, "
            + "t.leaseOwner = null, t.leaseUntil = null, t.updatedAt = :now "
            + "where t.id = :id and t.version = :version and t.status = :running")
    int markWaiting(@Param("id") String id, @Param("version") long version, @Param("next") TaskStatus next,
                    @Param("nextRunAt") Instant nextRunAt, @Param("refundAttempts") int refundAttempts,
                    @Param("addSystemRetries") int addSystemRetries, @Param("error") String error,
                    @Param("errorType") ErrorType errorType, @Param("processMs") long processMs,
                    @Param("now") Instant now, @Param("running") TaskStatus running);

    /** RUNNING -> FAILED or DEAD_LETTER. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update TaskEntity t set t.status = :terminal, t.finishedAt = :now, t.lastError = :error, "
            + "t.errorType = :errorType, t.processMs = t.processMs + :processMs, "
            + "t.leaseOwner = null, t.leaseUntil = null, t.updatedAt = :now "
            + "where t.id = :id and t.version = :version and t.status = :running")
    int markTerminalFailure(@Param("id") String id, @Param("version") long version,
                            @Param("terminal") TaskStatus terminal, @Param("error") String error,
                            @Param("errorType") ErrorType errorType, @Param("processMs") long processMs,
                            @Param("now") Instant now, @Param("running") TaskStatus running);

    /** Orphaned RUNNING rows that still have attempts left go back to PENDING. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update TaskEntity t set t.status = :pending, t.nextRunAt = :now, t.version = t.version + 1, "
            + "t.lastError = :error, t.errorType = :errorType, t.leaseOwner = null, t.leaseUntil = null, "
            + "t.updatedAt = :now "
            + "where t.status = :running and t.leaseUntil < :expiredBefore and t.attempts < t.maxAttempts")
    int requeueExpiredLeases(@Param("expiredBefore") Instant expiredBefore, @Param("now") Instant now,
                             @Param("error") String error, @Param("errorType") ErrorType errorType,
                             @Param("pending") TaskStatus pending, @Param("running") TaskStatus running);

    /** Orphaned RUNNING rows with no attempts left: a likely poison task, so dead-letter it. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update TaskEntity t set t.status = :deadLetter, t.finishedAt = :now, t.version = t.version + 1, "
            + "t.lastError = :error, t.errorType = :errorType, t.leaseOwner = null, t.leaseUntil = null, "
            + "t.updatedAt = :now "
            + "where t.status = :running and t.leaseUntil < :expiredBefore and t.attempts >= t.maxAttempts")
    int deadLetterExpiredLeases(@Param("expiredBefore") Instant expiredBefore, @Param("now") Instant now,
                                @Param("error") String error, @Param("errorType") ErrorType errorType,
                                @Param("deadLetter") TaskStatus deadLetter, @Param("running") TaskStatus running);

    /** FAILED/DEAD_LETTER -> PENDING with a fresh attempt budget (manual redrive). */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update TaskEntity t set t.status = :pending, t.attempts = 0, t.systemRetries = 0, t.nextRunAt = :now, "
            + "t.finishedAt = null, t.version = t.version + 1, t.updatedAt = :now "
            + "where t.id = :id and t.status in :redrivable")
    int redrive(@Param("id") String id, @Param("now") Instant now, @Param("pending") TaskStatus pending,
                @Param("redrivable") Collection<TaskStatus> redrivable);

    @Query("select t.id from TaskEntity t where t.status = :status and t.finishedAt < :before")
    List<String> findFinishedBefore(@Param("status") TaskStatus status, @Param("before") Instant before,
                                    Pageable limit);

    @Modifying
    @Query("delete from TaskEntity t where t.id in :ids")
    int deleteByIds(@Param("ids") Collection<String> ids);
}
