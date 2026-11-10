package io.github.yq808.taskqueue.domain;

/** Priority levels; the level number is what the queue and the database store. */
public enum TaskPriority {
    HIGH(0),
    NORMAL(1),
    LOW(2);

    private final int level;

    TaskPriority(int level) {
        this.level = level;
    }

    public int level() {
        return level;
    }

    public static TaskPriority fromLevel(int level) {
        for (TaskPriority p : values()) {
            if (p.level == level) {
                return p;
            }
        }
        throw new IllegalArgumentException("unknown priority level " + level);
    }

    public static int levelCount() {
        return values().length;
    }
}
