package io.github.yq808.taskqueue.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** The whole API against a real H2 file: every status a task can end in, and how each is reported. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@AutoConfigureObservability
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TaskLifecycleIntegrationTest {

    private static final String DATA_DIR = TestDataDir.create("lifecycle");

    @DynamicPropertySource
    static void dataDir(DynamicPropertyRegistry registry) {
        registry.add("taskqueue.data-dir", () -> DATA_DIR);
    }

    @Autowired
    private TestRestTemplate http;

    @Test
    void acceptsFastThenRunsAndReturnsTheResult() {
        ResponseEntity<JsonNode> response = submit(request("life-ok", Map.of("text", "Alpha beta gamma. Delta.")));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        String id = response.getBody().get("taskId").asText();
        assertThat(response.getHeaders().getLocation()).hasToString("/api/tasks/" + id);
        assertThat(response.getBody().get("status").asText()).isEqualTo("PENDING");

        JsonNode task = awaitTerminal(id);
        assertThat(task.get("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(task.get("attempts").asInt()).isEqualTo(1);
        assertThat(task.get("retries").asInt()).isZero();
        assertThat(task.get("resultAvailable").asBoolean()).isTrue();

        JsonNode result = http.getForObject("/api/tasks/" + id + "/result", JsonNode.class);
        assertThat(result.at("/result/summary").asText()).isEqualTo("Alpha beta gamma. Delta.");
        assertThat(result.at("/result/inputTokens").asInt()).isEqualTo(4);
    }

    @Test
    void resultIsNotAvailableBeforeTheTaskFinishes() {
        String id = submitId(request("life-pending", Map.of("text", "slow one", "latencyMs", 1500)));

        ResponseEntity<JsonNode> early = http.getForEntity("/api/tasks/" + id + "/result", JsonNode.class);
        assertThat(early.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(early.getBody().get("error").asText()).isEqualTo("task not finished yet");

        assertThat(http.getForEntity("/api/tasks/no-such-task", JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(http.getForEntity("/api/tasks/no-such-task/result", JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void sameBusinessKeyAndPayloadReturnsTheSameTask() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("text", "idempotent");
        payload.put("documentId", "d-1");
        ResponseEntity<JsonNode> first = submit(request("life-dup", payload));

        Map<String, Object> reordered = new LinkedHashMap<>();
        reordered.put("documentId", "d-1");
        reordered.put("text", "idempotent");
        ResponseEntity<JsonNode> second = submit(request("life-dup", reordered));

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().get("duplicate").asBoolean()).isTrue();
        assertThat(second.getBody().get("taskId")).isEqualTo(first.getBody().get("taskId"));
    }

    @Test
    void sameBusinessKeyWithADifferentPayloadIsAConflict() {
        String id = submitId(request("life-conflict", Map.of("text", "version one")));

        ResponseEntity<JsonNode> conflict = submit(request("life-conflict", Map.of("text", "version two")));

        assertThat(conflict.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(conflict.getBody().get("existingTaskId").asText()).isEqualTo(id);
    }

    @Test
    void retryableFailureIsRetriedThenSucceeds() {
        String id = submitId(request("life-fail-once", Map.of("text", "x", "simulate", "fail-once")));

        JsonNode task = awaitTerminal(id);

        assertThat(task.get("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(task.get("attempts").asInt()).isEqualTo(2);
        assertThat(task.get("retries").asInt()).isEqualTo(1);
        assertThat(outcomes(id)).containsExactly("RETRY", "SUCCEEDED");
    }

    @Test
    void exhaustedRetriesEndInDeadLetterWithTheReason() {
        Map<String, Object> body = request("life-fail-always", Map.of("text", "x", "simulate", "fail-always"));
        body.put("maxAttempts", 3);
        String id = submitId(body);

        JsonNode task = awaitTerminal(id);

        assertThat(task.get("status").asText()).isEqualTo("DEAD_LETTER");
        assertThat(task.get("attempts").asInt()).isEqualTo(3);
        assertThat(task.get("retries").asInt()).isEqualTo(2);
        assertThat(task.get("errorType").asText()).isEqualTo("BUSINESS_RETRYABLE");
        assertThat(task.get("lastError").asText()).startsWith("retries exhausted after 3 attempts");
        assertThat(outcomes(id)).containsExactly("RETRY", "RETRY", "DEAD_LETTER");
    }

    @Test
    void nonRetryableFailureFailsAtOnce() {
        String id = submitId(request("life-invalid", Map.of("text", "x", "simulate", "invalid")));

        JsonNode task = awaitTerminal(id);

        assertThat(task.get("status").asText()).isEqualTo("FAILED");
        assertThat(task.get("attempts").asInt()).isEqualTo(1);
        assertThat(task.get("errorType").asText()).isEqualTo("BUSINESS_FATAL");
        ResponseEntity<JsonNode> result = http.getForEntity("/api/tasks/" + id + "/result", JsonNode.class);
        assertThat(result.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(result.getBody().get("error").asText()).startsWith("task did not succeed");
    }

    @Test
    void unexpectedErrorIsDeadLetteredWithoutRetry() {
        String id = submitId(request("life-bug", Map.of("text", "x", "simulate", "bug")));

        JsonNode task = awaitTerminal(id);

        assertThat(task.get("status").asText()).isEqualTo("DEAD_LETTER");
        assertThat(task.get("attempts").asInt()).isEqualTo(1);
        assertThat(task.get("errorType").asText()).isEqualTo("SYSTEM_FATAL");
    }

    @Test
    void recoverableSystemFaultIsRetriedWithoutUsingAnAttempt() {
        String id = submitId(request("life-system", Map.of("text", "x", "simulate", "system-once")));

        JsonNode task = awaitTerminal(id);

        assertThat(task.get("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(task.get("attempts").asInt()).isEqualTo(1);
        assertThat(task.get("systemRetries").asInt()).isEqualTo(1);
    }

    @Test
    void anAttemptPastItsDeadlineIsARetryableTimeout() {
        Map<String, Object> body = request("life-slow", Map.of("text", "x", "simulate", "slow"));
        body.put("maxAttempts", 1);
        String id = submitId(body);

        JsonNode task = awaitTerminal(id);

        assertThat(task.get("status").asText()).isEqualTo("DEAD_LETTER");
        assertThat(task.get("lastError").asText()).contains("timed out");
    }

    @Test
    void redriveRunsAFailedTaskAgainAndRefusesOthers() {
        String failed = submitId(request("life-redrive", Map.of("text", "x", "simulate", "invalid")));
        awaitTerminal(failed);

        ResponseEntity<JsonNode> redrive = http.postForEntity("/api/tasks/" + failed + "/redrive", null, JsonNode.class);
        assertThat(redrive.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        await().atMost(Duration.ofSeconds(10)).until(() -> outcomes(failed).size() == 2);
        assertThat(awaitTerminal(failed).get("status").asText()).isEqualTo("FAILED");

        String ok = submitId(request("life-redrive-ok", Map.of("text", "fine")));
        awaitTerminal(ok);
        assertThat(http.postForEntity("/api/tasks/" + ok + "/redrive", null, JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void refusesMalformedRequests() {
        assertThat(submit(request("", Map.of("text", "x"))).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(submit(request("has space", Map.of("text", "x"))).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(submit(request("life-no-text", Map.of("documentId", "d"))).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);

        Map<String, Object> unknownType = request("life-unknown-type", Map.of("text", "x"));
        unknownType.put("type", "TRANSLATE");
        ResponseEntity<JsonNode> r = submit(unknownType);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(r.getBody().get("error").asText()).contains("unknown task type");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        assertThat(http.postForEntity("/api/tasks", new HttpEntity<>("{not json", headers), JsonNode.class)
                .getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void exposesStatsListsAndMetrics() {
        String id = submitId(request("life-observe", Map.of("text", "observe me")));
        awaitTerminal(id);

        JsonNode stats = http.getForObject("/api/tasks/stats", JsonNode.class);
        assertThat(stats.get("succeeded").asLong()).isPositive();
        assertThat(stats.has("backlog")).isTrue();
        assertThat(stats.has("running")).isTrue();
        assertThat(stats.has("failed")).isTrue();
        assertThat(stats.at("/byStatus/SUCCEEDED").asLong()).isEqualTo(stats.get("succeeded").asLong());

        JsonNode list = http.getForObject("/api/tasks?status=SUCCEEDED&size=200", JsonNode.class);
        assertThat(list.get("items")).allSatisfy(t -> assertThat(t.get("status").asText()).isEqualTo("SUCCEEDED"));

        JsonNode metrics = http.getForObject("/api/metrics", JsonNode.class);
        assertThat(metrics.at("/queue/capacity").asInt()).isEqualTo(32);
        assertThat(metrics.at("/workers/threads").asInt()).isEqualTo(4);
        assertThat(metrics.at("/counters/succeeded").asLong()).isPositive();
        assertThat(metrics.at("/timings/avgProcessingMs").asDouble()).isPositive();
        assertThat(metrics.has("admission")).isTrue();

        String prometheus = http.getForObject("/actuator/prometheus", String.class);
        assertThat(prometheus).contains("taskqueue_queue_size", "taskqueue_tasks_succeeded_total",
                "taskqueue_queue_wait_seconds", "taskqueue_workers_active");
    }

    // ---- helpers ----

    private static Map<String, Object> request(String bizKey, Map<String, Object> payload) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("bizKey", bizKey);
        body.put("payload", payload);
        return body;
    }

    private ResponseEntity<JsonNode> submit(Map<String, Object> body) {
        return http.postForEntity("/api/tasks", body, JsonNode.class);
    }

    private String submitId(Map<String, Object> body) {
        ResponseEntity<JsonNode> r = submit(body);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        return r.getBody().get("taskId").asText();
    }

    private JsonNode awaitTerminal(String id) {
        return await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(50))
                .until(() -> http.getForObject("/api/tasks/" + id, JsonNode.class),
                        t -> List.of("SUCCEEDED", "FAILED", "DEAD_LETTER").contains(t.get("status").asText()));
    }

    private List<String> outcomes(String id) {
        JsonNode task = http.getForObject("/api/tasks/" + id + "?history=true", JsonNode.class);
        return task.get("history").findValuesAsText("outcome");
    }
}
