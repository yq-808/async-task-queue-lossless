package io.github.yq808.taskqueue.web;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.yq808.taskqueue.domain.TaskPriority;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * @param bizKey      the caller's idempotency key, e.g. {@code doc-42:summary:v1}; one key = one task
 * @param type        task type, default {@code SUMMARIZE}
 * @param priority    HIGH, NORMAL (default) or LOW
 * @param payload     task input, checked by the type's handler
 * @param maxAttempts attempt budget for retryable failures (1..10), default from configuration
 */
public record SubmitTaskRequest(
        @NotBlank @Size(max = 128) @Pattern(regexp = "[A-Za-z0-9._:/-]+",
                message = "may contain letters, digits and . _ : / - only") String bizKey,
        String type,
        TaskPriority priority,
        @NotNull JsonNode payload,
        @Min(1) @Max(10) Integer maxAttempts) {
}
