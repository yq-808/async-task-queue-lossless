package io.github.yq808.taskqueue.engine;

import io.github.yq808.taskqueue.queue.Prioritized;

/**
 * What the in-memory queue holds: only an id and a priority. The task itself lives in the
 * database, which is the source of truth; the memory queue is a bounded, ordered index into it.
 */
public record QueuedTask(String id, int priorityLevel) implements Prioritized {
}
