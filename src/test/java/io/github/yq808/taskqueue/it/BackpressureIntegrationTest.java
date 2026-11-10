package io.github.yq808.taskqueue.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Backlog cap of 20 with one slow worker: LOW is refused at 10, NORMAL at 15, HIGH at 20, each
 * refusal a 429 with Retry-After, and the accepted backlog never runs past the cap.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "taskqueue.worker.threads=1",
        "taskqueue.admission.max-backlog=20",
        "taskqueue.admission.normal-share=0.75",
        "taskqueue.admission.low-share=0.5",
        "taskqueue.execution.timeout=5s",
        "taskqueue.simulation.min-latency=3s",
        "taskqueue.simulation.max-latency=3s"})
@ActiveProfiles("it")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class BackpressureIntegrationTest {

    private static final String DATA_DIR = TestDataDir.create("backpressure");

    @DynamicPropertySource
    static void dataDir(DynamicPropertyRegistry registry) {
        registry.add("taskqueue.data-dir", () -> DATA_DIR);
    }

    @Autowired
    private TestRestTemplate http;

    @Test
    void shedsLowPriorityFirstAndNeverExceedsTheCap() {
        int lowAccepted = burst("LOW", 15);
        int normalAccepted = burst("NORMAL", 15);
        int highAccepted = burst("HIGH", 15);

        // At most one task leaves the backlog during the burst (one worker, 3 s per task),
        // so each band may admit one extra once the periodic re-sync notices it.
        assertThat(lowAccepted).isBetween(10, 11);
        assertThat(lowAccepted + normalAccepted).isBetween(15, 16);
        assertThat(lowAccepted + normalAccepted + highAccepted).isBetween(20, 21);

        ResponseEntity<JsonNode> refused = submit("bp-extra", "HIGH");
        assertThat(refused.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(Long.parseLong(refused.getHeaders().getFirst(HttpHeaders.RETRY_AFTER))).isPositive();
        assertThat(refused.getBody().get("limitForPriority").asLong()).isEqualTo(20);

        JsonNode stats = http.getForObject("/api/tasks/stats", JsonNode.class);
        assertThat(stats.get("backlog").asLong() + stats.get("running").asLong()).isLessThanOrEqualTo(21);
        JsonNode metrics = http.getForObject("/api/metrics", JsonNode.class);
        assertThat(metrics.at("/counters/rejectedBacklog").asLong()).isPositive();
    }

    private int burst(String priority, int count) {
        int accepted = 0;
        for (int i = 0; i < count; i++) {
            ResponseEntity<JsonNode> r = submit("bp-" + priority + "-" + i, priority);
            if (r.getStatusCode() == HttpStatus.ACCEPTED) {
                accepted++;
            } else {
                assertThat(r.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
                assertThat(r.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isNotBlank();
            }
        }
        return accepted;
    }

    private ResponseEntity<JsonNode> submit(String key, String priority) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("bizKey", key);
        body.put("priority", priority);
        body.put("payload", Map.of("text", key));
        return http.postForEntity("/api/tasks", body, JsonNode.class);
    }
}
