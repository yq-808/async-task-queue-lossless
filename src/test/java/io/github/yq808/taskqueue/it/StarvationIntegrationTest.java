package io.github.yq808.taskqueue.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * A LOW task that spilled to disk while memory was full of HIGH work must not wait for the HIGH
 * flood to end: once it has waited past the aging threshold, a refill loads it ahead of them.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "taskqueue.worker.threads=1",
        "taskqueue.queue.capacity=4",
        "taskqueue.queue.aging-threshold=1s",
        "taskqueue.simulation.min-latency=50ms",
        "taskqueue.simulation.max-latency=50ms"})
@ActiveProfiles("it")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class StarvationIntegrationTest {

    private static final String DATA_DIR = TestDataDir.create("starvation");

    @DynamicPropertySource
    static void dataDir(DynamicPropertyRegistry registry) {
        registry.add("taskqueue.data-dir", () -> DATA_DIR);
    }

    @Autowired
    private TestRestTemplate http;

    @Test
    void aLowTaskOnDiskRunsBeforeTheHighFloodEnds() {
        String blocker = submit("starve-blocker", "NORMAL", 1000);
        await().atMost(Duration.ofSeconds(5)).until(() -> "RUNNING".equals(task(blocker).get("status").asText()));

        List<String> high = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            high.add(submit("starve-high-" + i, "HIGH", null)); // fills the 4 memory slots
        }
        String low = submit("starve-low", "LOW", null);         // memory full of HIGH: waits on disk
        for (int i = 4; i < 44; i++) {
            high.add(submit("starve-high-" + i, "HIGH", null)); // 40 more, about 2 s of work
        }
        await().atMost(Duration.ofSeconds(20)).until(() ->
                "SUCCEEDED".equals(task(low).get("status").asText())
                        && high.stream().allMatch(id -> "SUCCEEDED".equals(task(id).get("status").asText())));

        Instant lowStarted = Instant.parse(task(low).get("startedAt").asText());
        long highAfterLow = high.stream()
                .filter(id -> Instant.parse(task(id).get("startedAt").asText()).isAfter(lowStarted))
                .count();
        assertThat(highAfterLow).as("HIGH tasks that started after the starving LOW one").isGreaterThanOrEqualTo(10);
    }

    private String submit(String key, String priority, Integer latencyMs) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("text", key);
        if (latencyMs != null) {
            payload.put("latencyMs", latencyMs);
        }
        Map<String, Object> body = Map.of("bizKey", key, "priority", priority, "payload", payload);
        return http.postForObject("/api/tasks", body, JsonNode.class).get("taskId").asText();
    }

    private JsonNode task(String id) {
        return http.getForObject("/api/tasks/" + id, JsonNode.class);
    }
}
