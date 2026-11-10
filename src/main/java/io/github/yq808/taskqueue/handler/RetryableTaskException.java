package io.github.yq808.taskqueue.handler;

/** The task's own failure that may pass on a later try (model rate limit, timeout). */
public class RetryableTaskException extends RuntimeException {

    public RetryableTaskException(String message) {
        super(message);
    }
}
