package io.github.yq808.taskqueue.queue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * A bounded, multi-level priority blocking queue built only on {@link ReentrantLock} and
 * {@link Condition}. The JDK's {@code PriorityBlockingQueue} is unbounded, so it cannot give
 * backpressure; this one can.
 *
 * <ul>
 *   <li>One FIFO deque per priority level, so ordering inside a level is first-in first-out and
 *       every operation is O(levels).</li>
 *   <li>Strict priority, plus <b>aging</b>: when the head of a lower level has waited longer than
 *       {@code agingThresholdNanos}, it is served before newer urgent work, so low priority tasks
 *       cannot starve.</li>
 *   <li>Three ways to add, one per backpressure strategy: {@link #offer} rejects at once,
 *       {@link #offer(Prioritized, long, TimeUnit)} blocks the producer for a bounded time, and
 *       {@link #offerOrEvict} makes room by evicting the newest element of a strictly lower
 *       level (the caller must keep evicted elements somewhere durable).</li>
 *   <li>The capacity can be changed at runtime ({@link #setCapacity}); shrinking never drops
 *       elements, it only stops new ones until the queue drains below the new limit.</li>
 *   <li>{@link #close()} wakes every waiter; after that, adds fail and takers drain what is left
 *       and then get {@code null}.</li>
 * </ul>
 */
public final class BoundedPriorityBlockingQueue<E extends Prioritized> {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    private final ArrayDeque<Node<E>>[] levels;
    private final long agingThresholdNanos;
    private final LongSupplier nanoClock;

    private int capacity;
    private int count;
    private int peakSize;
    private boolean closed;

    public BoundedPriorityBlockingQueue(int capacity, int levelCount, long agingThresholdNanos) {
        this(capacity, levelCount, agingThresholdNanos, System::nanoTime);
    }

    @SuppressWarnings("unchecked")
    public BoundedPriorityBlockingQueue(int capacity, int levelCount, long agingThresholdNanos, LongSupplier nanoClock) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (levelCount <= 0) {
            throw new IllegalArgumentException("levelCount must be > 0");
        }
        this.capacity = capacity;
        this.levels = new ArrayDeque[levelCount];
        for (int i = 0; i < levelCount; i++) {
            levels[i] = new ArrayDeque<>();
        }
        this.agingThresholdNanos = agingThresholdNanos <= 0 ? Long.MAX_VALUE : agingThresholdNanos;
        this.nanoClock = Objects.requireNonNull(nanoClock);
    }

    /** Adds without waiting. Returns {@code false} when full or closed. */
    public boolean offer(E e) {
        Objects.requireNonNull(e);
        lock.lock();
        try {
            if (closed || count >= capacity) {
                return false;
            }
            enqueue(e);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Adds, waiting up to {@code timeout} for space (the "block the producer" strategy). */
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(e);
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (!closed && count >= capacity) {
                if (nanos <= 0L) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            if (closed) {
                return false;
            }
            enqueue(e);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Adds without waiting. When full, evicts the newest element of the lowest non-empty level if
     * that level is strictly less urgent than {@code e}.
     */
    public OfferResult<E> offerOrEvict(E e) {
        Objects.requireNonNull(e);
        lock.lock();
        try {
            if (closed) {
                return OfferResult.rejected();
            }
            if (count < capacity) {
                enqueue(e);
                return OfferResult.added();
            }
            int incoming = levelOf(e);
            for (int level = levels.length - 1; level > incoming; level--) {
                Node<E> victim = levels[level].pollLast();
                if (victim != null) {
                    count--;
                    enqueue(e);
                    return OfferResult.addedEvicting(victim.item);
                }
            }
            return OfferResult.rejected();
        } finally {
            lock.unlock();
        }
    }

    /** Waits up to {@code timeout} for an element. Returns {@code null} on timeout, or once closed and empty. */
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (count == 0) {
                if (closed || nanos <= 0L) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            return dequeue();
        } finally {
            lock.unlock();
        }
    }

    /** Waits for an element. Returns {@code null} once the queue is closed and empty. */
    public E take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (count == 0) {
                if (closed) {
                    return null;
                }
                notEmpty.await();
            }
            return dequeue();
        } finally {
            lock.unlock();
        }
    }

    /** Removes and returns every element, most urgent first. */
    public List<E> drain() {
        lock.lock();
        try {
            List<E> out = new ArrayList<>(count);
            for (ArrayDeque<Node<E>> level : levels) {
                Node<E> n;
                while ((n = level.pollFirst()) != null) {
                    out.add(n.item);
                }
            }
            count = 0;
            notFull.signalAll();
            return out;
        } finally {
            lock.unlock();
        }
    }

    /** Stops accepting elements and wakes every waiting producer and consumer. */
    public void close() {
        lock.lock();
        try {
            closed = true;
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void setCapacity(int newCapacity) {
        if (newCapacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        lock.lock();
        try {
            this.capacity = newCapacity;
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

    public int capacity() {
        lock.lock();
        try {
            return capacity;
        } finally {
            lock.unlock();
        }
    }

    public int remainingCapacity() {
        lock.lock();
        try {
            return Math.max(0, capacity - count);
        } finally {
            lock.unlock();
        }
    }

    /** Largest size ever observed; never exceeds the capacity in force when it was reached. */
    public int peakSize() {
        lock.lock();
        try {
            return peakSize;
        } finally {
            lock.unlock();
        }
    }

    public int[] sizeByLevel() {
        lock.lock();
        try {
            int[] sizes = new int[levels.length];
            for (int i = 0; i < levels.length; i++) {
                sizes[i] = levels[i].size();
            }
            return sizes;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The least urgent level that currently holds an element, or -1 when empty. Lets a caller
     * tell whether something more urgent waiting elsewhere could displace an element here.
     */
    public int leastUrgentLevelPresent() {
        lock.lock();
        try {
            for (int i = levels.length - 1; i >= 0; i--) {
                if (!levels[i].isEmpty()) {
                    return i;
                }
            }
            return -1;
        } finally {
            lock.unlock();
        }
    }

    /** Accepts elements again after {@link #close()}, for a component that is started a second time. */
    public void reopen() {
        lock.lock();
        try {
            closed = false;
        } finally {
            lock.unlock();
        }
    }

    public boolean isClosed() {
        lock.lock();
        try {
            return closed;
        } finally {
            lock.unlock();
        }
    }

    /** Age of the oldest element in nanoseconds, if any. */
    public Optional<Long> oldestAgeNanos() {
        lock.lock();
        try {
            long now = nanoClock.getAsLong();
            long oldest = Long.MAX_VALUE;
            for (ArrayDeque<Node<E>> level : levels) {
                Node<E> head = level.peekFirst();
                if (head != null) {
                    oldest = Math.min(oldest, head.enqueuedAt);
                }
            }
            return oldest == Long.MAX_VALUE ? Optional.empty() : Optional.of(now - oldest);
        } finally {
            lock.unlock();
        }
    }

    // ---- internals, called with the lock held ----

    private void enqueue(E e) {
        levels[levelOf(e)].addLast(new Node<>(e, nanoClock.getAsLong()));
        count++;
        if (count > peakSize) {
            peakSize = count;
        }
        notEmpty.signal();
    }

    private E dequeue() {
        int chosen = chooseLevel();
        Node<E> n = levels[chosen].pollFirst();
        count--;
        notFull.signal();
        return n.item;
    }

    /** Most urgent non-empty level, unless a less urgent head has waited past the aging threshold. */
    private int chooseLevel() {
        int first = -1;
        for (int i = 0; i < levels.length; i++) {
            if (!levels[i].isEmpty()) {
                first = i;
                break;
            }
        }
        long now = nanoClock.getAsLong();
        int starved = -1;
        long longestWait = -1;
        for (int i = first + 1; i < levels.length; i++) {
            Node<E> head = levels[i].peekFirst();
            if (head != null) {
                long waited = now - head.enqueuedAt;
                if (waited >= agingThresholdNanos && waited > longestWait) {
                    starved = i;
                    longestWait = waited;
                }
            }
        }
        return starved >= 0 ? starved : first;
    }

    private int levelOf(E e) {
        int level = e.priorityLevel();
        if (level < 0 || level >= levels.length) {
            throw new IllegalArgumentException("priority level out of range: " + level);
        }
        return level;
    }

    private record Node<E>(E item, long enqueuedAt) {
    }

    /** Outcome of {@link #offerOrEvict}. */
    public record OfferResult<E>(boolean accepted, E evicted) {

        static <E> OfferResult<E> added() {
            return new OfferResult<>(true, null);
        }

        static <E> OfferResult<E> addedEvicting(E evicted) {
            return new OfferResult<>(true, evicted);
        }

        static <E> OfferResult<E> rejected() {
            return new OfferResult<>(false, null);
        }

        public Optional<E> evictedElement() {
            return Optional.ofNullable(evicted);
        }
    }
}
