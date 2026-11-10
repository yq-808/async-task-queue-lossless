package io.github.yq808.taskqueue.domain;

/** Why an execution did not succeed; decides what happens next. */
public enum ErrorType {
    /** Task's own failure that may pass on retry (model rate limit, timeout). Retried with backoff, counts toward max attempts. */
    BUSINESS_RETRYABLE,
    /** Task's own failure that retrying cannot fix (invalid input). Goes straight to FAILED. */
    BUSINESS_FATAL,
    /** Infrastructure fault that should pass (downstream 503, database blip). Retried without using up business attempts, capped separately. */
    SYSTEM_RECOVERABLE,
    /** Unexpected error (a bug, an Error). Retrying would repeat it, so it goes to DEAD_LETTER for a human. */
    SYSTEM_FATAL,
    /** The worker vanished (process crash, hang) before finishing; found by lease expiry. */
    LEASE_EXPIRED,
    /** Interrupted by graceful shutdown; written back to PENDING without using up an attempt. */
    SHUTDOWN_INTERRUPTED
}
