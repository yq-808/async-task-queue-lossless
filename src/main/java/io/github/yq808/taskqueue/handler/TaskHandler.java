package io.github.yq808.taskqueue.handler;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * One kind of task. Handlers must be idempotent: delivery is at-least-once (a crash after the
 * work but before the result is committed runs the task again), so any side effect outside this
 * service should carry {@link TaskContext#taskId()} as its idempotency key.
 */
public interface TaskHandler {

    /** The task type this handler serves, e.g. {@code SUMMARIZE}. */
    String type();

    /** Checks a payload at submit time; throws {@link IllegalArgumentException} so a bad request is refused up front. */
    void validate(JsonNode payload);

    /**
     * Runs one attempt. Throw {@link RetryableTaskException}, {@link NonRetryableTaskException} or
     * {@link RecoverableSystemException} to say how a failure should be treated; anything else is
     * an unexpected error and is dead-lettered. Must respond to interruption (graceful shutdown).
     */
    JsonNode handle(TaskContext context) throws Exception;
}
