package io.github.yq808.taskqueue.queue;

/**
 * An element that can be placed in {@link BoundedPriorityBlockingQueue}.
 * Level 0 is the most urgent; larger levels are served later.
 */
public interface Prioritized {

    int priorityLevel();
}
