package io.github.yq808.taskqueue.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
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

/** With one worker kept busy, tasks queued meanwhile run HIGH first, then NORMAL, then LOW. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "taskqueue.worker.threads=1")
@ActiveProfiles("it")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PriorityOrderIntegrationTest {

    private static final String DATA_DIR = TestDataDir.create("priority");

    @DynamicPropertySource
    static void dataDir(DynamicPropertyRegistry registry) {
        registry.add("taskqueue.data-dir", () -> DATA_DIR);
    }

    @Autowired
    private TestRestTemplate http;

    @Test
    void urgentWorkOvertakesQueuedLessUrgentWork() {
        String blocker = submit("prio-blocker", "NORMAL", 800);
        await().atMost(Duration.ofSeconds(5)).until(() -> "RUNNING".equals(status(blocker)));

        List<String> submitted = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            submitted.add(submit("prio-low-" + i, "LOW", 10));
        }
        for (int i = 0; i < 3; i++) {
            submitted.add(submit("prio-normal-" + i, "NORMAL", 10));
        }
        for (int i = 0; i < 3; i++) {
            submitted.add(submit("prio-high-" + i, "HIGH", 10));
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> submitted.stream().allMatch(id -> "SUCCEEDED".equals(status(id))));

        List<String> startOrder = submitted.stream()
                .map(id -> http.getForObject("/api/tasks/" + id, JsonNode.class))
                .sorted(Comparator.comparing(t -> Instant.parse(t.get("startedAt").asText())))
                .map(t -> t.get("priority").asText())
                .toList();
        assertThat(startOrder).containsExactly("HIGH", "HIGH", "HIGH", "NORMAL", "NORMAL", "NORMAL", "LOW", "LOW", "LOW");
    }

    private String submit(String key, String priority, int latencyMs) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("bizKey", key);
        body.put("priority", priority);
        body.put("payload", Map.of("text", key, "latencyMs", latencyMs));
        return http.postForObject("/api/tasks", body, JsonNode.class).get("taskId").asText();
    }

    private String status(String id) {
        return http.getForObject("/api/tasks/" + id, JsonNode.class).get("status").asText();
    }
}
