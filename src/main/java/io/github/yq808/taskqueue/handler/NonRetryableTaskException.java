package io.github.yq808.taskqueue.handler;

/** The task's own failure that no retry can fix (the input itself is bad). */
public class NonRetryableTaskException extends RuntimeException {

    public NonRetryableTaskException(String message) {
        super(message);
    }
}
