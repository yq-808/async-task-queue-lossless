package io.github.yq808.taskqueue.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yq808.taskqueue.domain.TaskPriority;
import io.github.yq808.taskqueue.domain.TaskStatus;
import io.github.yq808.taskqueue.engine.AdmissionController;
import io.github.yq808.taskqueue.engine.TaskDispatcher;
import io.github.yq808.taskqueue.engine.TaskSubmissionService;
import io.github.yq808.taskqueue.engine.TaskSubmissionService.SubmitResult;
import io.github.yq808.taskqueue.handler.SummarizeTaskHandler;
import io.github.yq808.taskqueue.persistence.TaskEntity;
import io.github.yq808.taskqueue.persistence.TaskStore;
import jakarta.validation.Valid;
import java.net.URI;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskSubmissionService submissions;
    private final TaskStore store;
    private final TaskDispatcher dispatcher;
    private final AdmissionController admission;
    private final ObjectMapper mapper;
    private final Clock clock;

    public TaskController(TaskSubmissionService submissions, TaskStore store, TaskDispatcher dispatcher,
                          AdmissionController admission, ObjectMapper mapper, Clock clock) {
        this.submissions = submissions;
        this.store = store;
        this.dispatcher = dispatcher;
        this.admission = admission;
        this.mapper = mapper;
        this.clock = clock;
    }

    /**
     * 202 Accepted (new task), 200 OK (same business key and payload: the existing task),
     * 409 Conflict (same key, different payload), 429 Too Many Requests (backpressure),
     * 503 Service Unavailable (shutting down). 429 and 503 carry Retry-After.
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> submit(@Valid @RequestBody SubmitTaskRequest request) {
        var cmd = new TaskSubmissionService.SubmitCommand(request.bizKey(),
                request.type() == null ? SummarizeTaskHandler.TYPE : request.type(),
                request.priority() == null ? TaskPriority.NORMAL : request.priority(),
                request.payload(), request.maxAttempts());
        SubmitResult result = submissions.submit(cmd);
        if (result instanceof SubmitResult.Accepted accepted) {
            return ResponseEntity.accepted().location(URI.create("/api/tasks/" + accepted.task().getId()))
                    .body(ticket(accepted.task(), false));
        }
        if (result instanceof SubmitResult.Duplicate duplicate) {
            return ResponseEntity.ok(ticket(duplicate.task(), true));
        }
        if (result instanceof SubmitResult.Conflict conflict) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(body(
                    "error", "business key already used with a different payload",
                    "bizKey", request.bizKey(),
                    "existingTaskId", conflict.existing().getId()));
        }
        AdmissionController.Decision d = ((SubmitResult.Rejected) result).decision();
        boolean stopping = d.outcome() == AdmissionController.Decision.Outcome.SHUTTING_DOWN;
        return ResponseEntity.status(stopping ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.TOO_MANY_REQUESTS)
                .header(HttpHeaders.RETRY_AFTER, String.valueOf(d.retryAfterSeconds()))
                .body(stopping
                        ? body("error", "shutting down, not accepting tasks", "retryAfterSeconds", d.retryAfterSeconds())
                        : body("error", "backlog full for priority " + cmd.priority(),
                                "backlog", d.backlog(), "limitForPriority", d.threshold(),
                                "retryAfterSeconds", d.retryAfterSeconds()));
    }

    @GetMapping("/{id}")
    public ResponseEntity<TaskView> get(@PathVariable String id,
                                        @RequestParam(defaultValue = "false") boolean history) {
        return store.find(id)
                .map(t -> ResponseEntity.ok(TaskView.of(t, history ? store.attemptHistory(id) : null)))
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * 200 with the result once SUCCEEDED; 409 with the current status otherwise; 404 for an unknown
     * id, including a task that retention deleted, even between the two reads here.
     */
    @GetMapping("/{id}/result")
    public ResponseEntity<Map<String, Object>> result(@PathVariable String id) throws JsonProcessingException {
        TaskEntity task = store.find(id).orElse(null);
        if (task == null) {
            return ResponseEntity.notFound().build();
        }
        if (task.getStatus() != TaskStatus.SUCCEEDED) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(body(
                    "taskId", id, "status", task.getStatus(),
                    "error", task.getStatus().isTerminal() ? "task did not succeed: " + task.getLastError()
                            : "task not finished yet"));
        }
        var result = store.findResult(id).orElse(null);
        if (result == null) {
            // Deleted by retention between the two reads.
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(body("taskId", id, "status", task.getStatus(),
                "result", mapper.readTree(result.getResult()), "completedAt", result.getCreatedAt()));
    }

    @GetMapping
    public Map<String, Object> list(@RequestParam(required = false) TaskStatus status,
                                    @RequestParam(defaultValue = "0") int page,
                                    @RequestParam(defaultValue = "20") int size) {
        Page<TaskEntity> p = store.list(status, Math.max(0, page), Math.min(Math.max(1, size), 200));
        return body("page", p.getNumber(), "size", p.getSize(), "total", p.getTotalElements(),
                "items", p.getContent().stream().map(t -> TaskView.of(t, null)).toList());
    }

    /** Counts by status, plus the three figures operators ask for first: backlog, running, failed. */
    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<TaskStatus, Long> counts = store.countByStatus();
        long backlog = counts.get(TaskStatus.PENDING) + counts.get(TaskStatus.RETRY_WAIT);
        return body("backlog", backlog,
                "inMemoryQueue", dispatcher.size(),
                "running", counts.get(TaskStatus.RUNNING),
                "retryWaiting", counts.get(TaskStatus.RETRY_WAIT),
                "succeeded", counts.get(TaskStatus.SUCCEEDED),
                "failed", counts.get(TaskStatus.FAILED) + counts.get(TaskStatus.DEAD_LETTER),
                "deadLetter", counts.get(TaskStatus.DEAD_LETTER),
                "byStatus", counts,
                "accepting", admission.isAccepting());
    }

    /** Puts a FAILED or DEAD_LETTER task back to PENDING with a fresh attempt budget. */
    @PostMapping("/{id}/redrive")
    public ResponseEntity<Map<String, Object>> redrive(@PathVariable String id) {
        TaskEntity task = store.find(id).orElse(null);
        if (task == null) {
            return ResponseEntity.notFound().build();
        }
        if (!store.redrive(id, clock.instant())) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(body(
                    "error", "only FAILED or DEAD_LETTER tasks can be redriven", "status", task.getStatus()));
        }
        admission.returnedToBacklog(1);
        dispatcher.offerNew(id, task.getPriority());
        return ResponseEntity.accepted().body(body("taskId", id, "status", TaskStatus.PENDING));
    }

    private static Map<String, Object> ticket(TaskEntity t, boolean duplicate) {
        return body("taskId", t.getId(), "bizKey", t.getBizKey(), "status", t.getStatus(), "duplicate", duplicate,
                "statusUrl", "/api/tasks/" + t.getId(), "resultUrl", "/api/tasks/" + t.getId() + "/result");
    }

    private static Map<String, Object> body(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }
}
