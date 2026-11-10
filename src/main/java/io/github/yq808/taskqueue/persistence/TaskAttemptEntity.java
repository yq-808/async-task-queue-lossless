package io.github.yq808.taskqueue.persistence;

import io.github.yq808.taskqueue.domain.ErrorType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/** One row per execution, so a task's failure history is visible, not just its last error. */
@Entity
@Table(name = "task_attempt")
public class TaskAttemptEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "task_id", nullable = false)
    private String taskId;

    @Column(name = "attempt_no", nullable = false)
    private int attemptNo;

    @Column(nullable = false)
    private String outcome;

    @Enumerated(EnumType.STRING)
    @Column(name = "error_type")
    private ErrorType errorType;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(nullable = false)
    private String worker;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "finished_at", nullable = false)
    private Instant finishedAt;

    @Column(name = "duration_ms", nullable = false)
    private long durationMs;

    protected TaskAttemptEntity() {
    }

    public TaskAttemptEntity(String taskId, int attemptNo, String outcome, ErrorType errorType, String errorMessage,
                             String worker, Instant startedAt, Instant finishedAt) {
        this.taskId = taskId;
        this.attemptNo = attemptNo;
        this.outcome = outcome;
        this.errorType = errorType;
        this.errorMessage = errorMessage;
        this.worker = worker;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.durationMs = Math.max(0, finishedAt.toEpochMilli() - startedAt.toEpochMilli());
    }

    public Long getId() {
        return id;
    }

    public String getTaskId() {
        return taskId;
    }

    public int getAttemptNo() {
        return attemptNo;
    }

    public String getOutcome() {
        return outcome;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getWorker() {
        return worker;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public long getDurationMs() {
        return durationMs;
    }
}
