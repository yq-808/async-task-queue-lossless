package io.github.yq808.taskqueue.persistence;

import java.time.Instant;

/** The few columns the dispatcher needs to load a waiting task into memory. */
public record DispatchCandidate(String id, int priority, Instant readyAt) {
}
