package io.github.yq808.taskqueue.handler;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * What a handler gets for one attempt.
 *
 * @param attempt       1-based business attempt number
 * @param systemRetries how many earlier runs ended in a recoverable system fault
 * @param deadline      the attempt must finish by then; past it, throw a retryable timeout
 */
public record TaskContext(String taskId, String bizKey, int attempt, int systemRetries, JsonNode payload,
                          Instant deadline, Clock clock) {

    public Duration remaining() {
        Duration left = Duration.between(clock.instant(), deadline);
        return left.isNegative() ? Duration.ZERO : left;
    }
}
