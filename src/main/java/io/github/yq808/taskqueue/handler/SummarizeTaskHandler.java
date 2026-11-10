package io.github.yq808.taskqueue.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.github.yq808.taskqueue.config.TaskQueueProperties;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * Simulated "summarize a document with an LLM" task, the scenario this queue is built for.
 *
 * <p>Cost profile of one attempt: a short CPU phase (tokenising and fingerprinting the text,
 * proportional to its size) followed by a long IO wait (the model call, 200ms–1.5s by default).
 * Wait time dominates by roughly 50:1, which is why the worker pool is sized by the downstream
 * concurrency quota rather than by CPU cores.
 *
 * <p>Failures are simulated at configurable rates, or forced per task with
 * {@code payload.simulate}: {@code invalid}, {@code fail-once}, {@code fail-always},
 * {@code system-once}, {@code bug}, {@code slow}.
 */
@Component
public class SummarizeTaskHandler implements TaskHandler {

    public static final String TYPE = "SUMMARIZE";
    private static final int MAX_TEXT_CHARS = 200_000;

    private final TaskQueueProperties.Simulation simulation;
    private final ObjectMapper mapper;
    private final AtomicLong invocations = new AtomicLong();

    public SummarizeTaskHandler(TaskQueueProperties properties, ObjectMapper mapper) {
        this.simulation = properties.simulation();
        this.mapper = mapper;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public void validate(JsonNode payload) {
        if (payload == null || !payload.isObject()) {
            throw new IllegalArgumentException("payload must be a JSON object");
        }
        JsonNode text = payload.get("text");
        if (text == null || !text.isTextual() || text.asText().isBlank()) {
            throw new IllegalArgumentException("payload.text is required");
        }
        if (text.asText().length() > MAX_TEXT_CHARS) {
            throw new IllegalArgumentException("payload.text exceeds " + MAX_TEXT_CHARS + " characters");
        }
        JsonNode max = payload.get("maxSummaryChars");
        if (max != null && (!max.canConvertToInt() || max.asInt() < 20 || max.asInt() > 2000)) {
            throw new IllegalArgumentException("payload.maxSummaryChars must be between 20 and 2000");
        }
    }

    @Override
    public JsonNode handle(TaskContext ctx) throws Exception {
        invocations.incrementAndGet();
        JsonNode p = ctx.payload();
        String mode = p.path("simulate").asText("");
        String text = p.path("text").asText();

        if ("invalid".equals(mode)) {
            throw new NonRetryableTaskException("document could not be parsed (unsupported encoding)");
        }
        if ("bug".equals(mode)) {
            throw new IllegalStateException("simulated defect: tokenizer returned null");
        }

        // CPU phase: tokenise and fingerprint the text.
        long started = System.nanoTime();
        int tokens = countTokens(text);
        String digest = fingerprint(text);

        // IO phase: wait for the (simulated) model service, bounded by the attempt deadline.
        Duration latency = latencyFor(p, mode, ctx);
        Duration remaining = ctx.remaining();
        if (latency.compareTo(remaining) > 0) {
            Thread.sleep(remaining.toMillis());
            throw new RetryableTaskException("model call timed out after " + remaining.toMillis() + "ms");
        }
        Thread.sleep(latency.toMillis());

        switch (mode) {
            case "fail-once" -> {
                if (ctx.attempt() == 1) {
                    throw new RetryableTaskException("model rate limited (HTTP 429)");
                }
            }
            case "fail-always" -> throw new RetryableTaskException("model rate limited (HTTP 429)");
            case "system-once" -> {
                if (ctx.systemRetries() == 0) {
                    throw new RecoverableSystemException("model gateway unavailable (HTTP 503)");
                }
            }
            case "" -> randomFailure();
            default -> {
                // other modes only change latency
            }
        }

        int maxChars = p.path("maxSummaryChars").asInt(200);
        ObjectNode result = mapper.createObjectNode();
        result.put("documentId", p.path("documentId").asText(ctx.bizKey()));
        result.put("model", p.path("model").asText("sim-llm-1"));
        result.put("summary", summarize(text, maxChars));
        result.put("inputTokens", tokens);
        result.put("outputTokens", Math.max(1, Math.min(tokens, maxChars / 4)));
        result.put("contentSha256", digest);
        result.put("modelLatencyMs", latency.toMillis());
        result.put("totalMs", Duration.ofNanos(System.nanoTime() - started).toMillis());
        return result;
    }

    /** Number of times {@link #handle} has run, used by tests to prove nothing ran twice. */
    public long invocations() {
        return invocations.get();
    }

    private Duration latencyFor(JsonNode p, String mode, TaskContext ctx) {
        if ("slow".equals(mode)) {
            return ctx.remaining().plusSeconds(1);
        }
        if (p.hasNonNull("latencyMs")) {
            return Duration.ofMillis(Math.max(0, p.get("latencyMs").asLong()));
        }
        long min = simulation.minLatency().toMillis();
        long max = Math.max(min, simulation.maxLatency().toMillis());
        return Duration.ofMillis(min == max ? min : ThreadLocalRandom.current().nextLong(min, max + 1));
    }

    private void randomFailure() {
        double r = ThreadLocalRandom.current().nextDouble();
        if (r < simulation.retryableFailureRate()) {
            throw new RetryableTaskException("model rate limited (HTTP 429)");
        }
        if (r < simulation.retryableFailureRate() + simulation.systemFailureRate()) {
            throw new RecoverableSystemException("model gateway unavailable (HTTP 503)");
        }
    }

    private static int countTokens(String text) {
        int tokens = 0;
        boolean inWord = false;
        for (int i = 0; i < text.length(); i++) {
            boolean ws = Character.isWhitespace(text.charAt(i));
            if (!ws && !inWord) {
                tokens++;
            }
            inWord = !ws;
        }
        return tokens;
    }

    private String fingerprint(String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        int rounds = Math.max(1, simulation.cpuWorkRounds() * (1 + bytes.length / 1024));
        byte[] current = bytes;
        for (int i = 0; i < rounds; i++) {
            Hasher h = Hashing.sha256().newHasher();
            h.putBytes(current);
            h.putInt(i);
            current = h.hash().asBytes();
        }
        return Hashing.sha256().hashBytes(current).toString();
    }

    private static String summarize(String text, int maxChars) {
        String flat = text.strip().replaceAll("\\s+", " ");
        if (flat.length() <= maxChars) {
            return flat;
        }
        int cut = flat.lastIndexOf(' ', maxChars - 1);
        return flat.substring(0, cut > maxChars / 2 ? cut : maxChars - 1) + "…";
    }
}
