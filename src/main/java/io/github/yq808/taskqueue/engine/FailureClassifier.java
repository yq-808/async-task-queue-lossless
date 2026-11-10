package io.github.yq808.taskqueue.engine;

import io.github.yq808.taskqueue.domain.ErrorType;
import io.github.yq808.taskqueue.handler.NonRetryableTaskException;
import io.github.yq808.taskqueue.handler.RecoverableSystemException;
import io.github.yq808.taskqueue.handler.RetryableTaskException;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.springframework.dao.TransientDataAccessException;

/**
 * Maps a failure to what happens next.
 *
 * <ul>
 *   <li>Task's own, may pass ({@link RetryableTaskException}, {@link TimeoutException}, or a model
 *       answer that could not be parsed, {@link JsonProcessingException}, since a model can answer
 *       differently next time): retry with backoff, uses an attempt.</li>
 *   <li>Task's own, will not pass ({@link NonRetryableTaskException}, {@link IllegalArgumentException}): FAILED at once.</li>
 *   <li>System, recoverable ({@link RecoverableSystemException}, transient database errors, an interrupt
 *       that did not come from shutdown, or any failure caused by an IO error, also when wrapped, as in
 *       {@link java.io.UncheckedIOException} or an HTTP client's resource-access exception): retry without
 *       using up the task's attempts, capped separately, and counted by the circuit breaker.</li>
 *   <li>System, unrecoverable (anything else: a bug, an {@link Error}): DEAD_LETTER, since re-running a defect
 *       only repeats it.</li>
 * </ul>
 */
public final class FailureClassifier {

    private FailureClassifier() {
    }

    public static ErrorType classify(Throwable t) {
        Throwable cause = unwrap(t);
        if (cause instanceof RetryableTaskException || cause instanceof TimeoutException
                || cause instanceof JsonProcessingException) {
            return ErrorType.BUSINESS_RETRYABLE;
        }
        if (cause instanceof NonRetryableTaskException || cause instanceof IllegalArgumentException) {
            return ErrorType.BUSINESS_FATAL;
        }
        if (cause instanceof RecoverableSystemException || cause instanceof TransientDataAccessException
                || cause instanceof InterruptedException || causedByIo(cause)) {
            return ErrorType.SYSTEM_RECOVERABLE;
        }
        return ErrorType.SYSTEM_FATAL;
    }

    public static String describe(Throwable t) {
        Throwable cause = unwrap(t);
        String message = cause.getMessage();
        return cause.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    /** True when an IO error (other than a parse error) is anywhere in the cause chain. */
    private static boolean causedByIo(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if (c instanceof IOException && !(c instanceof JsonProcessingException)) {
                return true;
            }
        }
        return false;
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
