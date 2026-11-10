package io.github.yq808.taskqueue.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.yq808.taskqueue.engine.TaskDispatcher;
import io.github.yq808.taskqueue.handler.SummarizeTaskHandler;
import io.github.yq808.taskqueue.metrics.TaskMetrics;
import io.github.yq808.taskqueue.persistence.TaskResultRepository;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Many concurrent clients, a deliberately tiny memory queue (8 slots for 8 workers), and a third
 * of the requests repeating a business key. Proves: one task per key, every accepted task runs
 * exactly once (no loss, no double execution), and the memory bound holds while the overflow
 * waits on disk.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "taskqueue.worker.threads=8",
        "taskqueue.queue.capacity=8",
        "taskqueue.simulation.min-latency=5ms",
        "taskqueue.simulation.max-latency=20ms"})
@ActiveProfiles("it")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ConcurrencyIntegrationTest {

    private static final String DATA_DIR = TestDataDir.create("concurrency");
    private static final int DISTINCT_KEYS = 1000;
    private static final int DUPLICATE_REQUESTS = 500;

    @DynamicPropertySource
    static void dataDir(DynamicPropertyRegistry registry) {
        registry.add("taskqueue.data-dir", () -> DATA_DIR);
    }

    @Autowired
    private TestRestTemplate http;
    @Autowired
    private SummarizeTaskHandler handler;
    @Autowired
    private TaskResultRepository results;
    @Autowired
    private TaskDispatcher dispatcher;
    @Autowired
    private TaskMetrics metrics;

    @Test
    void concurrentSubmissionsRunExactlyOnceEach() throws Exception {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < DISTINCT_KEYS; i++) {
            keys.add("conc-" + i);
        }
        for (int i = 0; i < DUPLICATE_REQUESTS; i++) {
            keys.add("conc-" + (i * 2));
        }
        Collections.shuffle(keys);

        Map<String, Set<String>> idsByKey = new ConcurrentHashMap<>();
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        String[] priorities = {"HIGH", "NORMAL", "LOW"};
        ExecutorService clients = Executors.newFixedThreadPool(32);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (String key : keys) {
            futures.add(clients.submit(() -> {
                start.await();
                int n = Integer.parseInt(key.substring(5));
                Map<String, Object> body = Map.of("bizKey", key, "priority", priorities[n % 3],
                        "payload", Map.of("text", "document number " + n));
                ResponseEntity<JsonNode> r = http.postForEntity("/api/tasks", body, JsonNode.class);
                switch (r.getStatusCode().value()) {
                    case 202 -> accepted.incrementAndGet();
                    case 200 -> duplicates.incrementAndGet();
                    default -> other.incrementAndGet();
                }
                if (r.getBody() != null && r.getBody().has("taskId")) {
                    idsByKey.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(r.getBody().get("taskId").asText());
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        clients.shutdown();

        assertThat(other.get()).as("no request may fail").isZero();
        assertThat(accepted.get()).isEqualTo(DISTINCT_KEYS);
        assertThat(duplicates.get()).isEqualTo(DUPLICATE_REQUESTS);
        assertThat(idsByKey).hasSize(DISTINCT_KEYS);
        assertThat(idsByKey.values()).allSatisfy(ids -> assertThat(ids).hasSize(1));

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200)).until(() ->
                http.getForObject("/api/tasks/stats", JsonNode.class).get("succeeded").asLong() == DISTINCT_KEYS);

        assertThat(handler.invocations()).as("each task ran exactly once").isEqualTo(DISTINCT_KEYS);
        assertThat(results.count()).isEqualTo(DISTINCT_KEYS);
        assertThat(dispatcher.peakSize()).as("memory queue bound").isLessThanOrEqualTo(8);
        assertThat(metrics.spilledToDisk.count()).as("overflow waited on disk").isPositive();
        JsonNode stats = http.getForObject("/api/tasks/stats", JsonNode.class);
        assertThat(stats.get("backlog").asLong()).isZero();
        assertThat(stats.get("running").asLong()).isZero();
    }
}
