package io.github.yq808.taskqueue.queue;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/** Many producers and consumers against a small queue: nothing lost, nothing duplicated, bound kept. */
class BoundedPriorityBlockingQueueConcurrencyTest {

    record Item(int id, int priorityLevel) implements Prioritized {
    }

    @RepeatedTest(3)
    void blockingProducersAndConsumersLoseAndDuplicateNothing() throws Exception {
        int producers = 8;
        int consumers = 8;
        int perProducer = 5_000;
        int capacity = 32;
        var q = new BoundedPriorityBlockingQueue<Item>(capacity, 3, TimeUnit.MILLISECONDS.toNanos(5));
        Set<Integer> seen = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicates = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(producers + consumers);

        List<Future<?>> producerFutures = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            int base = p * perProducer;
            producerFutures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perProducer; i++) {
                    int id = base + i;
                    if (!q.offer(new Item(id, id % 3), 10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("offer timed out for " + id);
                    }
                }
                return null;
            }));
        }
        List<Future<?>> consumerFutures = new ArrayList<>();
        for (int c = 0; c < consumers; c++) {
            consumerFutures.add(pool.submit(() -> {
                start.await();
                Item item;
                while ((item = q.take()) != null) {
                    if (!seen.add(item.id())) {
                        duplicates.incrementAndGet();
                    }
                }
                return null;
            }));
        }

        start.countDown();
        for (Future<?> f : producerFutures) {
            f.get(60, TimeUnit.SECONDS);
        }
        q.close();
        for (Future<?> f : consumerFutures) {
            f.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertThat(duplicates.get()).isZero();
        assertThat(seen).hasSize(producers * perProducer);
        assertThat(q.size()).isZero();
        assertThat(q.peakSize()).isLessThanOrEqualTo(capacity);
    }

    @Test
    void concurrentEvictingOffersNeverExceedCapacity() throws Exception {
        int capacity = 16;
        var q = new BoundedPriorityBlockingQueue<Item>(capacity, 3, 0);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger evicted = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            int base = t * 1_000;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < 1_000; i++) {
                    var r = q.offerOrEvict(new Item(base + i, (base + i) % 3));
                    if (r.accepted()) {
                        accepted.incrementAndGet();
                    }
                    if (r.evictedElement().isPresent()) {
                        evicted.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertThat(q.peakSize()).isEqualTo(capacity);
        assertThat(q.size()).isEqualTo(accepted.get() - evicted.get());
        assertThat(q.sizeByLevel()[2]).as("a full queue under pressure keeps the urgent work").isZero();
    }
}
