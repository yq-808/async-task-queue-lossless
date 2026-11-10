package io.github.yq808.taskqueue.engine;

import io.github.yq808.taskqueue.domain.ErrorType;
import io.github.yq808.taskqueue.handler.NonRetryableTaskException;
import io.github.yq808.taskqueue.handler.RecoverableSystemException;
import io.github.yq808.taskqueue.handler.RetryableTaskException;
import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.springframework.dao.TransientDataAccessException;

/**
 * Maps a failure to what happens next.
 *
 * <ul>
 *   <li>Task's own, may pass ({@link RetryableTaskException}, {@link TimeoutException}): retry with backoff, uses an attempt.</li>
 *   <li>Task's own, will not pass ({@link NonRetryableTaskException}, {@link IllegalArgumentException}): FAILED at once.</li>
 *   <li>System, recoverable ({@link RecoverableSystemException}, IO errors, transient database errors, an
 *       interrupt that did not come from shutdown): retry
 *       without using up the task's attempts, capped separately, and counted by the circuit breaker.</li>
 *   <li>System, unrecoverable (anything else: a bug, an {@link Error}): DEAD_LETTER, since re-running a defect
 *       only repeats it.</li>
 * </ul>
 */
public final class FailureClassifier {

    private FailureClassifier() {
    }

    public static ErrorType classify(Throwable t) {
        Throwable cause = unwrap(t);
        if (cause instanceof RetryableTaskException || cause instanceof TimeoutException) {
            return ErrorType.BUSINESS_RETRYABLE;
        }
        if (cause instanceof NonRetryableTaskException || cause instanceof IllegalArgumentException) {
            return ErrorType.BUSINESS_FATAL;
        }
        if (cause instanceof RecoverableSystemException || cause instanceof IOException
                || cause instanceof TransientDataAccessException || cause instanceof InterruptedException) {
            return ErrorType.SYSTEM_RECOVERABLE;
        }
        return ErrorType.SYSTEM_FATAL;
    }

    public static String describe(Throwable t) {
        Throwable cause = unwrap(t);
        String message = cause.getMessage();
        return cause.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    private static Throwable unwrap(Throwable t) {
        Throwable current = t;
        while ((current instanceof ExecutionException || current instanceof CompletionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
