package io.github.yq808.taskqueue.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class BoundedPriorityBlockingQueueTest {

    record Item(String id, int priorityLevel) implements Prioritized {
    }

    private static BoundedPriorityBlockingQueue<Item> queue(int capacity) {
        return new BoundedPriorityBlockingQueue<>(capacity, 3, 0);
    }

    @Test
    void servesHigherPriorityFirstAndFifoWithinALevel() throws Exception {
        var q = queue(10);
        q.offer(new Item("low-1", 2));
        q.offer(new Item("normal-1", 1));
        q.offer(new Item("high-1", 0));
        q.offer(new Item("normal-2", 1));
        q.offer(new Item("high-2", 0));

        List<String> order = List.of(
                q.take().id(), q.take().id(), q.take().id(), q.take().id(), q.take().id());

        assertThat(order).containsExactly("high-1", "high-2", "normal-1", "normal-2", "low-1");
    }

    @Test
    void rejectsWhenFull() {
        var q = queue(2);
        assertThat(q.offer(new Item("a", 1))).isTrue();
        assertThat(q.offer(new Item("b", 1))).isTrue();
        assertThat(q.offer(new Item("c", 0))).isFalse();
        assertThat(q.size()).isEqualTo(2);
        assertThat(q.remainingCapacity()).isZero();
    }

    @Test
    void timedOfferBlocksUntilSpaceFreesUp() throws Exception {
        var q = queue(1);
        q.offer(new Item("a", 1));

        CompletableFuture<Boolean> producer = CompletableFuture.supplyAsync(() -> {
            try {
                return q.offer(new Item("b", 1), 5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        Thread.sleep(100);
        assertThat(producer).isNotDone();

        assertThat(q.take().id()).isEqualTo("a");
        assertThat(producer.get(5, TimeUnit.SECONDS)).isTrue();
        assertThat(q.take().id()).isEqualTo("b");
    }

    @Test
    void timedOfferGivesUpAfterTimeout() throws Exception {
        var q = queue(1);
        q.offer(new Item("a", 1));
        long start = System.nanoTime();
        assertThat(q.offer(new Item("b", 1), 50, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isGreaterThanOrEqualTo(45);
    }

    @Test
    void offerOrEvictReplacesTheNewestLeastUrgentElement() throws Exception {
        var q = queue(3);
        q.offer(new Item("low-old", 2));
        q.offer(new Item("low-new", 2));
        q.offer(new Item("normal", 1));

        var result = q.offerOrEvict(new Item("high", 0));

        assertThat(result.accepted()).isTrue();
        assertThat(result.evictedElement()).map(Item::id).contains("low-new");
        assertThat(q.take().id()).isEqualTo("high");
        assertThat(q.take().id()).isEqualTo("normal");
        assertThat(q.take().id()).isEqualTo("low-old");
    }

    @Test
    void offerOrEvictNeverEvictsEqualOrHigherPriority() {
        var q = queue(2);
        q.offer(new Item("normal-1", 1));
        q.offer(new Item("high", 0));

        var result = q.offerOrEvict(new Item("normal-2", 1));

        assertThat(result.accepted()).isFalse();
        assertThat(result.evictedElement()).isEmpty();
        assertThat(q.size()).isEqualTo(2);
    }

    @Test
    void agingServesAStarvedLowPriorityElement() throws Exception {
        AtomicLong clock = new AtomicLong();
        var q = new BoundedPriorityBlockingQueue<Item>(10, 3, TimeUnit.SECONDS.toNanos(30), clock::get);
        q.offer(new Item("low", 2));
        clock.addAndGet(TimeUnit.SECONDS.toNanos(10));
        q.offer(new Item("high-1", 0));

        assertThat(q.take().id()).as("low has only waited 10s").isEqualTo("high-1");

        q.offer(new Item("high-2", 0));
        clock.addAndGet(TimeUnit.SECONDS.toNanos(25));

        assertThat(q.take().id()).as("low has now waited 35s").isEqualTo("low");
        assertThat(q.take().id()).isEqualTo("high-2");
    }

    @Test
    void closeWakesWaitingConsumersAndLetsThemDrain() throws Exception {
        var q = queue(5);
        CompletableFuture<Item> waiting = CompletableFuture.supplyAsync(() -> {
            try {
                return q.take();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        Thread.sleep(50);
        q.close();
        assertThat(waiting.get(2, TimeUnit.SECONDS)).isNull();
        assertThat(q.offer(new Item("late", 0))).isFalse();
    }

    @Test
    void closedQueueStillHandsOutRemainingElements() throws Exception {
        var q = queue(5);
        q.offer(new Item("a", 1));
        q.close();
        assertThat(q.poll(10, TimeUnit.MILLISECONDS).id()).isEqualTo("a");
        assertThat(q.poll(10, TimeUnit.MILLISECONDS)).isNull();
    }

    @Test
    void drainReturnsEverythingMostUrgentFirst() {
        var q = queue(5);
        q.offer(new Item("low", 2));
        q.offer(new Item("high", 0));
        q.offer(new Item("normal", 1));

        assertThat(q.drain()).extracting(Item::id).containsExactly("high", "normal", "low");
        assertThat(q.size()).isZero();
    }

    @Test
    void shrinkingCapacityKeepsElementsButBlocksNewOnes() throws Exception {
        var q = queue(4);
        q.offer(new Item("a", 1));
        q.offer(new Item("b", 1));
        q.offer(new Item("c", 1));
        q.setCapacity(2);

        assertThat(q.size()).isEqualTo(3);
        assertThat(q.offer(new Item("d", 1))).isFalse();
        q.take();
        q.take();
        assertThat(q.offer(new Item("d", 1))).isTrue();
    }

    @Test
    void rejectsOutOfRangePriority() {
        var q = queue(2);
        assertThatThrownBy(() -> q.offer(new Item("x", 3))).isInstanceOf(IllegalArgumentException.class);
    }
}
