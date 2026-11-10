package io.github.yq808.taskqueue.persistence;

import io.github.yq808.taskqueue.domain.ErrorType;
import io.github.yq808.taskqueue.domain.TaskStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Task state row. Every state change after creation goes through a conditional bulk update in
 * {@link TaskRepository} (status and {@code version} in the WHERE clause), never through
 * load-modify-save, so two writers can never both win.
 */
@Entity
@Table(name = "task")
public class TaskEntity {

    @Id
    private String id;

    @Column(name = "biz_key", nullable = false, updatable = false)
    private String bizKey;

    @Column(name = "task_type", nullable = false, updatable = false)
    private String taskType;

    @Column(nullable = false, updatable = false)
    private int priority;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private TaskStatus status;

    @Column(nullable = false, updatable = false)
    private String payload;

    @Column(name = "payload_hash", nullable = false, updatable = false)
    private String payloadHash;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "max_attempts", nullable = false)
    private int maxAttempts;

    @Column(name = "system_retries", nullable = false)
    private int systemRetries;

    @Column(name = "last_error")
    private String lastError;

    @Enumerated(EnumType.STRING)
    @Column(name = "error_type")
    private ErrorType errorType;

    @Column(name = "next_run_at", nullable = false)
    private Instant nextRunAt;

    @Column(name = "lease_owner")
    private String leaseOwner;

    @Column(name = "lease_until")
    private Instant leaseUntil;

    @Column(nullable = false)
    private long version;

    @Column(name = "queue_wait_ms", nullable = false)
    private long queueWaitMs;

    @Column(name = "process_ms", nullable = false)
    private long processMs;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected TaskEntity() {
    }

    public static TaskEntity newPending(String id, String bizKey, String taskType, int priority, String payload,
                                        String payloadHash, int maxAttempts, Instant now) {
        TaskEntity t = new TaskEntity();
        t.id = id;
        t.bizKey = bizKey;
        t.taskType = taskType;
        t.priority = priority;
        t.status = TaskStatus.PENDING;
        t.payload = payload;
        t.payloadHash = payloadHash;
        t.maxAttempts = maxAttempts;
        t.nextRunAt = now;
        t.createdAt = now;
        t.updatedAt = now;
        return t;
    }

    public String getId() {
        return id;
    }

    public String getBizKey() {
        return bizKey;
    }

    public String getTaskType() {
        return taskType;
    }

    public int getPriority() {
        return priority;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public String getPayload() {
        return payload;
    }

    public String getPayloadHash() {
        return payloadHash;
    }

    public int getAttempts() {
        return attempts;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public int getSystemRetries() {
        return systemRetries;
    }

    public String getLastError() {
        return lastError;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public Instant getNextRunAt() {
        return nextRunAt;
    }

    public String getLeaseOwner() {
        return leaseOwner;
    }

    public Instant getLeaseUntil() {
        return leaseUntil;
    }

    public long getVersion() {
        return version;
    }

    public long getQueueWaitMs() {
        return queueWaitMs;
    }

    public long getProcessMs() {
        return processMs;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
