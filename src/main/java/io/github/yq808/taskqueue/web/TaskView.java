package io.github.yq808.taskqueue.web;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.yq808.taskqueue.domain.ErrorType;
import io.github.yq808.taskqueue.domain.TaskPriority;
import io.github.yq808.taskqueue.domain.TaskStatus;
import io.github.yq808.taskqueue.persistence.TaskAttemptEntity;
import io.github.yq808.taskqueue.persistence.TaskEntity;
import java.time.Instant;
import java.util.List;

/**
 * @param retries       how many times the task has been retried (attempts beyond the first)
 * @param queueWaitMs   total time spent runnable but not running
 * @param processMs     total time spent running, over all attempts
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TaskView(String taskId, String bizKey, String type, TaskPriority priority, TaskStatus status,
                       int attempts, int maxAttempts, int retries, int systemRetries, ErrorType errorType,
                       String lastError, Instant createdAt, Instant startedAt, Instant finishedAt,
                       Instant nextRunAt, long queueWaitMs, long processMs, boolean resultAvailable,
                       List<AttemptView> history) {

    public static TaskView of(TaskEntity t, List<TaskAttemptEntity> history) {
        return new TaskView(t.getId(), t.getBizKey(), t.getTaskType(), TaskPriority.fromLevel(t.getPriority()),
                t.getStatus(), t.getAttempts(), t.getMaxAttempts(), Math.max(0, t.getAttempts() - 1),
                t.getSystemRetries(), t.getErrorType(), t.getLastError(), t.getCreatedAt(), t.getStartedAt(),
                t.getFinishedAt(), t.getStatus().isTerminal() ? null : t.getNextRunAt(), t.getQueueWaitMs(),
                t.getProcessMs(), t.getStatus() == TaskStatus.SUCCEEDED,
                history == null ? null : history.stream().map(AttemptView::of).toList());
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record AttemptView(int attempt, String outcome, ErrorType errorType, String error, String worker,
                              Instant startedAt, long durationMs) {

        static AttemptView of(TaskAttemptEntity a) {
            return new AttemptView(a.getAttemptNo(), a.getOutcome(), a.getErrorType(), a.getErrorMessage(),
                    a.getWorker(), a.getStartedAt(), a.getDurationMs());
        }
    }
}
