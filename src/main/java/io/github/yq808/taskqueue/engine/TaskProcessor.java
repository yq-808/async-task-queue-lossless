package io.github.yq808.taskqueue.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yq808.taskqueue.config.TaskQueueProperties;
import io.github.yq808.taskqueue.domain.ErrorType;
import io.github.yq808.taskqueue.domain.TaskStatus;
import io.github.yq808.taskqueue.handler.TaskContext;
import io.github.yq808.taskqueue.handler.TaskHandler;
import io.github.yq808.taskqueue.handler.TaskHandlerRegistry;
import io.github.yq808.taskqueue.metrics.TaskMetrics;
import io.github.yq808.taskqueue.persistence.ClaimedTask;
import io.github.yq808.taskqueue.persistence.TaskStore;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Runs queued tasks: claim one, call its handler, then record the outcome.
 *
 * <pre>
 *   success                          -> SUCCEEDED + result row (one transaction)
 *   BUSINESS_RETRYABLE, attempts left -> RETRY_WAIT, next_run_at = now + backoff
 *   BUSINESS_RETRYABLE, none left     -> DEAD_LETTER
 *   BUSINESS_FATAL                    -> FAILED
 *   SYSTEM_RECOVERABLE                -> RETRY_WAIT, attempt refunded, system_retries + 1 (DEAD_LETTER past the cap)
 *   SYSTEM_FATAL                      -> DEAD_LETTER
 *   interrupted by shutdown           -> PENDING, attempt refunded (written back for the next start)
 * </pre>
 *
 * Every write is fenced by the claim's version, so a worker that lost its lease (it hung past
 * the lease and the task was handed to someone else) cannot overwrite the newer outcome.
 */
@Component
public class TaskProcessor {

    private static final Logger log = LoggerFactory.getLogger(TaskProcessor.class);

    private final TaskStore store;
    private final TaskHandlerRegistry handlers;
    private final RetryPolicy retryPolicy;
    private final DownstreamCircuitBreaker breaker;
    private final AdmissionController admission;
    private final TaskMetrics metrics;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final Duration timeout;
    private final Duration lease;

    public TaskProcessor(TaskStore store, TaskHandlerRegistry handlers, RetryPolicy retryPolicy,
                         DownstreamCircuitBreaker breaker, AdmissionController admission, TaskMetrics metrics,
                         ObjectMapper mapper, Clock clock, TaskQueueProperties properties) {
        this.store = store;
        this.handlers = handlers;
        this.retryPolicy = retryPolicy;
        this.breaker = breaker;
        this.admission = admission;
        this.metrics = metrics;
        this.mapper = mapper;
        this.clock = clock;
        this.timeout = properties.execution().timeout();
        this.lease = properties.execution().lease();
        if (lease.compareTo(timeout) <= 0) {
            throw new IllegalStateException("taskqueue.execution.lease must be longer than taskqueue.execution.timeout");
        }
    }

    /** PENDING/RETRY_WAIT -> RUNNING under a lease. Empty if the task is no longer runnable. */
    public Optional<ClaimedTask> claim(QueuedTask queued, WorkerSlot slot) {
        Optional<ClaimedTask> claim = store.claim(queued.id(), slot.name(), clock.instant(), lease);
        if (claim.isEmpty()) {
            metrics.claimMisses.increment();
        } else {
            metrics.recordQueueWait(claim.get().queueWaitMs());
        }
        return claim;
    }

    /** Runs a claimed task's handler and records the outcome. */
    public void execute(ClaimedTask task, WorkerSlot slot) {
        Instant claimedAt = task.claimedAt();
        long startNanos = System.nanoTime();
        JsonNode result = null;
        Throwable failure = null;
        boolean shutdownInterrupt = false;
        try {
            TaskHandler handler = handlers.find(task.taskType())
                    .orElseThrow(() -> new IllegalStateException("no handler registered for type " + task.taskType()));
            JsonNode payload = parse(task);
            TaskContext ctx = new TaskContext(task.id(), task.bizKey(), task.attemptNo(), task.systemRetries(),
                    payload, claimedAt.plus(timeout), clock);
            slot.enterHandler();
            try {
                result = handler.handle(ctx);
            } finally {
                shutdownInterrupt = slot.exitHandler();
            }
        } catch (Throwable t) {
            failure = t;
        }
        long processMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        metrics.recordProcessing(processMs);
        Instant now = clock.instant();

        if (failure == null) {
            onSuccess(task, result, processMs, slot, now);
        } else {
            onFailure(task, failure, shutdownInterrupt, processMs, slot, now);
            if (failure instanceof VirtualMachineError error) {
                throw error;
            }
        }
    }

    private JsonNode parse(ClaimedTask task) {
        try {
            return mapper.readTree(task.payload());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("stored payload is not valid JSON", e);
        }
    }

    private void onSuccess(ClaimedTask task, JsonNode result, long processMs, WorkerSlot slot, Instant now) {
        String json;
        try {
            json = mapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            // Our own result could not be serialised: a defect, so dead-letter rather than retry.
            onFailure(task, new IllegalStateException("result could not be serialised", e), false, processMs, slot, now);
            return;
        }
        if (store.complete(task, json, processMs, slot.name(), now)) {
            metrics.succeeded.increment();
            metrics.onTaskFinished();
            breaker.onSuccess();
        } else {
            fencedOut(task);
        }
    }

    private void onFailure(ClaimedTask task, Throwable failure, boolean shutdownInterrupt, long processMs,
                           WorkerSlot slot, Instant now) {
        String error = FailureClassifier.describe(failure);
        if (shutdownInterrupt) {
            boolean ok = store.reschedule(task, TaskStatus.PENDING, now, true, false,
                    ErrorType.SHUTDOWN_INTERRUPTED, "interrupted by graceful shutdown, written back: " + error,
                    processMs, slot.name(), now);
            if (ok) {
                metrics.writtenBack.increment();
                admission.returnedToBacklog(1);
            } else {
                fencedOut(task);
            }
            return;
        }

        ErrorType type = FailureClassifier.classify(failure);
        boolean ok;
        switch (type) {
            case BUSINESS_RETRYABLE -> {
                if (task.attemptNo() < task.maxAttempts()) {
                    Instant next = now.plus(retryPolicy.backoff(task.attemptNo()));
                    ok = store.reschedule(task, TaskStatus.RETRY_WAIT, next, false, false, type, error, processMs,
                            slot.name(), now);
                    if (ok) {
                        metrics.retriesScheduled.increment();
                        admission.returnedToBacklog(1);
                    }
                } else {
                    ok = deadLetter(task, type, "retries exhausted after " + task.attemptNo() + " attempts: " + error,
                            processMs, slot, now);
                }
            }
            case SYSTEM_RECOVERABLE -> {
                breaker.onSystemFailure();
                int systemRetries = task.systemRetries() + 1;
                if (systemRetries <= retryPolicy.maxSystemRetries()) {
                    Instant next = now.plus(retryPolicy.backoff(systemRetries));
                    ok = store.reschedule(task, TaskStatus.RETRY_WAIT, next, true, true, type, error, processMs,
                            slot.name(), now);
                    if (ok) {
                        metrics.systemRetries.increment();
                        admission.returnedToBacklog(1);
                    }
                } else {
                    ok = deadLetter(task, type, "system retries exhausted after " + task.systemRetries()
                            + " re-runs: " + error, processMs, slot, now);
                }
            }
            case BUSINESS_FATAL -> {
                ok = store.fail(task, TaskStatus.FAILED, type, error, processMs, slot.name(), now);
                if (ok) {
                    metrics.failed.increment();
                    metrics.onTaskFinished();
                }
            }
            default -> {
                log.error("Task {} hit an unexpected error and is dead-lettered", task.id(), failure);
                ok = deadLetter(task, type, error, processMs, slot, now);
            }
        }
        if (!ok) {
            fencedOut(task);
        }
    }

    private boolean deadLetter(ClaimedTask task, ErrorType type, String error, long processMs, WorkerSlot slot,
                               Instant now) {
        boolean ok = store.fail(task, TaskStatus.DEAD_LETTER, type, error, processMs, slot.name(), now);
        if (ok) {
            metrics.deadLettered.increment();
            metrics.onTaskFinished();
        }
        return ok;
    }

    private void fencedOut(ClaimedTask task) {
        metrics.fencedOut.increment();
        log.warn("Task {} attempt {} lost its lease before finishing; its outcome was discarded", task.id(),
                task.attemptNo());
    }
}
