package io.github.yq808.taskqueue.persistence;

import java.time.Instant;

/**
 * A task a worker holds. {@code version} is the fencing token: every later write for this
 * execution must match it, so a worker that lost its lease cannot overwrite the next owner.
 */
public record ClaimedTask(String id, String bizKey, String taskType, int priority, String payload,
                          int attemptNo, int maxAttempts, int systemRetries, long version,
                          Instant claimedAt, long queueWaitMs) {
}
