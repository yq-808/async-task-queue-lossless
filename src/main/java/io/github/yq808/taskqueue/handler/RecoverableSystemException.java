package io.github.yq808.taskqueue.handler;

/** An infrastructure fault that is expected to pass (downstream outage, connection reset). */
public class RecoverableSystemException extends RuntimeException {

    public RecoverableSystemException(String message) {
        super(message);
    }

    public RecoverableSystemException(String message, Throwable cause) {
        super(message, cause);
    }
}
