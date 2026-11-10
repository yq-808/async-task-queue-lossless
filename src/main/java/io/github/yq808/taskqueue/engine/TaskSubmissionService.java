package io.github.yq808.taskqueue.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.hash.Hashing;
import io.github.yq808.taskqueue.config.TaskQueueProperties;
import io.github.yq808.taskqueue.domain.TaskPriority;
import io.github.yq808.taskqueue.handler.TaskHandler;
import io.github.yq808.taskqueue.handler.TaskHandlerRegistry;
import io.github.yq808.taskqueue.metrics.TaskMetrics;
import io.github.yq808.taskqueue.persistence.TaskEntity;
import io.github.yq808.taskqueue.persistence.TaskStore;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * The receiving side. A submission is answered as soon as one INSERT has committed; the work
 * itself happens later on a worker.
 *
 * <p>Order of checks:
 * <ol>
 *   <li>validate the request (unknown type or bad payload -> 400)</li>
 *   <li>business key already known: same payload -> the existing task id (idempotent);
 *       different payload -> conflict (409)</li>
 *   <li>admission control (backlog over the cap -> 429 + Retry-After; stopping -> 503)</li>
 *   <li>INSERT ... status PENDING and commit. <b>This commit is the "accepted" boundary</b>:
 *       from here on the task survives a process crash.</li>
 *   <li>offer it to the in-memory queue (fast path); if memory is full it simply waits on disk</li>
 * </ol>
 * Two concurrent submissions with the same key race on the unique index; the loser reads the
 * winner's row and answers as a duplicate or conflict, so one key never yields two tasks.
 */
@Service
public class TaskSubmissionService {

    private final TaskHandlerRegistry handlers;
    private final TaskStore store;
    private final AdmissionController admission;
    private final TaskDispatcher dispatcher;
    private final TaskMetrics metrics;
    private final Clock clock;
    private final int defaultMaxAttempts;
    private final ObjectMapper canonicalMapper;

    public TaskSubmissionService(TaskHandlerRegistry handlers, TaskStore store, AdmissionController admission,
                                 TaskDispatcher dispatcher, TaskMetrics metrics, Clock clock, ObjectMapper mapper,
                                 TaskQueueProperties properties) {
        this.handlers = handlers;
        this.store = store;
        this.admission = admission;
        this.dispatcher = dispatcher;
        this.metrics = metrics;
        this.clock = clock;
        this.defaultMaxAttempts = properties.retry().maxAttempts();
        this.canonicalMapper = mapper.copy().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    public SubmitResult submit(SubmitCommand cmd) {
        TaskHandler handler = handlers.find(cmd.type())
                .orElseThrow(() -> new IllegalArgumentException("unknown task type '" + cmd.type()
                        + "', known types: " + handlers.types()));
        handler.validate(cmd.payload());
        String payload = canonicalJson(cmd.payload());
        String hash = Hashing.sha256().hashString(cmd.type() + "\n" + payload, StandardCharsets.UTF_8).toString();

        Optional<TaskEntity> existing = store.findByBizKey(cmd.bizKey());
        if (existing.isPresent()) {
            return answerExisting(existing.get(), hash);
        }

        AdmissionController.Decision decision = admission.tryAdmit(cmd.priority());
        if (!decision.admitted()) {
            if (decision.outcome() == AdmissionController.Decision.Outcome.SHUTTING_DOWN) {
                metrics.rejectedShutdown.increment();
            } else {
                metrics.rejectedBacklog.increment();
            }
            return new SubmitResult.Rejected(decision);
        }

        int maxAttempts = cmd.maxAttempts() != null ? cmd.maxAttempts() : defaultMaxAttempts;
        TaskEntity task = TaskEntity.newPending(UUID.randomUUID().toString(), cmd.bizKey(), cmd.type(),
                cmd.priority().level(), payload, hash, maxAttempts, clock.instant());
        try {
            store.insert(task);
        } catch (RuntimeException e) {
            admission.release();
            Optional<TaskEntity> winner = store.findByBizKey(cmd.bizKey());
            if (winner.isPresent()) {
                return answerExisting(winner.get(), hash);
            }
            throw e;
        }
        metrics.submitted.increment();
        dispatcher.offerNew(task.getId(), task.getPriority());
        return new SubmitResult.Accepted(task);
    }

    private SubmitResult answerExisting(TaskEntity existing, String hash) {
        if (existing.getPayloadHash().equals(hash)) {
            metrics.duplicates.increment();
            return new SubmitResult.Duplicate(existing);
        }
        metrics.conflicts.increment();
        return new SubmitResult.Conflict(existing);
    }

    /** Key-sorted JSON, so {"a":1,"b":2} and {"b":2,"a":1} count as the same payload. */
    private String canonicalJson(JsonNode payload) {
        try {
            Object tree = canonicalMapper.treeToValue(payload, Object.class);
            return canonicalMapper.writeValueAsString(tree);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("payload is not valid JSON", e);
        }
    }

    public record SubmitCommand(String bizKey, String type, TaskPriority priority, JsonNode payload,
                                Integer maxAttempts) {
    }

    public sealed interface SubmitResult {

        record Accepted(TaskEntity task) implements SubmitResult {
        }

        record Duplicate(TaskEntity task) implements SubmitResult {
        }

        record Conflict(TaskEntity existing) implements SubmitResult {
        }

        record Rejected(AdmissionController.Decision decision) implements SubmitResult {
        }
    }
}
