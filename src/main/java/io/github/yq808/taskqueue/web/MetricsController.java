package io.github.yq808.taskqueue.web;

import io.github.yq808.taskqueue.domain.TaskPriority;
import io.github.yq808.taskqueue.engine.AdmissionController;
import io.github.yq808.taskqueue.engine.DownstreamCircuitBreaker;
import io.github.yq808.taskqueue.engine.TaskDispatcher;
import io.github.yq808.taskqueue.engine.WorkerPool;
import io.github.yq808.taskqueue.metrics.TaskMetrics;
import io.micrometer.core.instrument.Counter;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * One-page operational view. The same figures are also published as Micrometer meters under
 * {@code taskqueue.*} at /actuator/metrics and /actuator/prometheus.
 */
@RestController
public class MetricsController {

    private final TaskDispatcher dispatcher;
    private final WorkerPool workers;
    private final AdmissionController admission;
    private final DownstreamCircuitBreaker breaker;
    private final TaskMetrics metrics;

    public MetricsController(TaskDispatcher dispatcher, WorkerPool workers, AdmissionController admission,
                             DownstreamCircuitBreaker breaker, TaskMetrics metrics) {
        this.dispatcher = dispatcher;
        this.workers = workers;
        this.admission = admission;
        this.breaker = breaker;
        this.metrics = metrics;
    }

    @GetMapping("/api/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> out = new LinkedHashMap<>();

        int[] byLevel = dispatcher.sizeByPriority();
        Map<String, Integer> byPriority = new LinkedHashMap<>();
        for (TaskPriority p : TaskPriority.values()) {
            byPriority.put(p.name(), byLevel[p.level()]);
        }
        Map<String, Object> queue = new LinkedHashMap<>();
        queue.put("size", dispatcher.size());
        queue.put("capacity", dispatcher.capacity());
        queue.put("lowWatermark", dispatcher.lowWatermark());
        queue.put("peakSize", dispatcher.peakSize());
        queue.put("sizeByPriority", byPriority);
        queue.put("oldestQueuedMs", dispatcher.oldestQueuedMillis());
        out.put("queue", queue);

        Map<String, Object> workerView = new LinkedHashMap<>();
        workerView.put("threads", workers.threads());
        workerView.put("active", workers.activeCount());
        workerView.put("circuitBreakerOpen", breaker.isOpen());
        out.put("workers", workerView);

        Map<String, Object> admissionView = new LinkedHashMap<>();
        admissionView.put("accepting", admission.isAccepting());
        admissionView.put("persistedBacklog", admission.backlog());
        admissionView.put("backlogLimit", admission.currentLimit());
        admissionView.put("throughputPerSec", round(metrics.throughputPerSecond()));
        out.put("admission", admissionView);

        Map<String, Object> counters = new LinkedHashMap<>();
        counters.put("submitted", count(metrics.submitted));
        counters.put("duplicates", count(metrics.duplicates));
        counters.put("conflicts", count(metrics.conflicts));
        counters.put("rejectedBacklog", count(metrics.rejectedBacklog));
        counters.put("rejectedShutdown", count(metrics.rejectedShutdown));
        counters.put("succeeded", count(metrics.succeeded));
        counters.put("failed", count(metrics.failed));
        counters.put("deadLettered", count(metrics.deadLettered));
        counters.put("retries", count(metrics.retriesScheduled));
        counters.put("systemRetries", count(metrics.systemRetries));
        counters.put("recoveredAfterCrash", count(metrics.recovered));
        counters.put("writtenBackOnShutdown", count(metrics.writtenBack));
        counters.put("spilledToDisk", count(metrics.spilledToDisk));
        counters.put("claimMisses", count(metrics.claimMisses));
        counters.put("fencedOut", count(metrics.fencedOut));
        counters.put("breakerOpened", count(metrics.breakerOpened));
        out.put("counters", counters);

        var wait = metrics.queueWaitSummary();
        var proc = metrics.processingSummary();
        Map<String, Object> timings = new LinkedHashMap<>();
        timings.put("avgQueueWaitMs", round(wait.avgMs()));
        timings.put("p95QueueWaitMs", round(wait.p95Ms()));
        timings.put("maxQueueWaitMs", round(wait.maxMs()));
        timings.put("avgProcessingMs", round(proc.avgMs()));
        timings.put("p95ProcessingMs", round(proc.p95Ms()));
        timings.put("maxProcessingMs", round(proc.maxMs()));
        timings.put("attemptsTimed", proc.count());
        out.put("timings", timings);
        return out;
    }

    private static long count(Counter c) {
        return (long) c.count();
    }

    private static double round(double v) {
        return Math.round(v * 10.0) / 10.0;
    }
}
