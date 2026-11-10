package io.github.yq808.taskqueue.domain;

import java.util.EnumSet;
import java.util.Set;

public enum TaskStatus {
    /** Persisted and waiting to run (in the in-memory queue or only on disk). */
    PENDING,
    /** Failed, waiting for its backoff to pass before it can run again. */
    RETRY_WAIT,
    /** Claimed by a worker under a lease. */
    RUNNING,
    SUCCEEDED,
    /** A business failure that retrying cannot fix (for example invalid input). */
    FAILED,
    /** Retries exhausted or an unrecoverable system error; kept for inspection and manual redrive. */
    DEAD_LETTER;

    /** Statuses a task can be dispatched from. */
    public static final Set<TaskStatus> WAITING = EnumSet.of(PENDING, RETRY_WAIT);

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == DEAD_LETTER;
    }
}
