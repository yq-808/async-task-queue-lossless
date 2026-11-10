package io.github.yq808.taskqueue.persistence;

import io.github.yq808.taskqueue.domain.TaskStatus;

public record StatusCount(TaskStatus status, long count) {
}
