package io.github.yq808.taskqueue.engine;

/**
 * One worker thread's interrupt gate. Shutdown may interrupt a worker only while it is inside
 * the handler, never while it is writing to the database: an interrupt during an H2 file write
 * can close the store's file channel and take the database down with it. Entering, leaving and
 * interrupting all hold the same monitor, so an interrupt cannot slip in after the handler has
 * returned.
 */
public final class WorkerSlot {

    private final String name;
    private Thread thread;
    private boolean inHandler;
    private boolean interruptRequested;

    public WorkerSlot(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    synchronized void bind(Thread t) {
        this.thread = t;
    }

    public synchronized void enterHandler() {
        inHandler = true;
        interruptRequested = false;
    }

    /**
     * Leaves the handler and clears any interrupt, so the database writes that follow run on a
     * clean thread. Returns whether shutdown interrupted this run.
     */
    public synchronized boolean exitHandler() {
        inHandler = false;
        Thread.interrupted();
        boolean requested = interruptRequested;
        interruptRequested = false;
        return requested;
    }

    /** Interrupts the worker if, and only if, it is inside the handler right now. */
    synchronized boolean interruptIfInHandler() {
        if (inHandler && thread != null) {
            interruptRequested = true;
            thread.interrupt();
            return true;
        }
        return false;
    }
}
