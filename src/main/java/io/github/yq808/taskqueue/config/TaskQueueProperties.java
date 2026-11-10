package io.github.yq808.taskqueue.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Every knob of the queue, under {@code taskqueue.*}. The defaults are reasoned in docs/DESIGN.md.
 */
@ConfigurationProperties("taskqueue")
public record TaskQueueProperties(
        @DefaultValue Worker worker,
        @DefaultValue Queue queue,
        @DefaultValue Admission admission,
        @DefaultValue Retry retry,
        @DefaultValue Execution execution,
        @DefaultValue Dispatcher dispatcher,
        @DefaultValue Shutdown shutdown,
        @DefaultValue Retention retention,
        @DefaultValue Simulation simulation) {

    /**
     * @param threads worker threads. The work is mostly waiting on a model service, so this is
     *                sized by the downstream concurrency quota, not by CPU cores.
     */
    public record Worker(@DefaultValue("16") int threads) {
    }

    /**
     * @param capacity          in-memory queue capacity; 0 means {@code threads * capacityFactor}
     * @param capacityFactor    queue slots per worker when capacity is 0
     * @param lowWatermarkRatio the dispatcher refills from the database when the queue is at or below this share of capacity
     * @param agingThreshold    a lower priority task that has waited this long is served before newer urgent ones
     */
    public record Queue(@DefaultValue("0") int capacity,
                        @DefaultValue("8") int capacityFactor,
                        @DefaultValue("0.5") double lowWatermarkRatio,
                        @DefaultValue("30s") Duration agingThreshold) {

        public int effectiveCapacity(int workerThreads) {
            return capacity > 0 ? capacity : Math.max(1, workerThreads * capacityFactor);
        }
    }

    /**
     * Backpressure at the front door: the persisted backlog (PENDING + RETRY_WAIT) is capped.
     *
     * @param maxBacklog         hard cap on the backlog
     * @param adaptive           when true, the cap shrinks to {@code throughput * targetMaxQueueWait} so a
     *                           newly accepted task can still be served within that time
     * @param minBacklog         the adaptive cap never goes below this
     * @param targetMaxQueueWait queueing time a caller should be willing to accept
     * @param normalShare        NORMAL tasks are rejected once the backlog reaches this share of the cap
     * @param lowShare           LOW tasks are rejected once the backlog reaches this share of the cap
     */
    public record Admission(@DefaultValue("10000") long maxBacklog,
                            @DefaultValue("true") boolean adaptive,
                            @DefaultValue("1000") long minBacklog,
                            @DefaultValue("10m") Duration targetMaxQueueWait,
                            @DefaultValue("0.9") double normalShare,
                            @DefaultValue("0.7") double lowShare) {
    }

    /**
     * @param maxAttempts      default attempt budget for business-retryable failures (a request may lower or raise it, 1..10)
     * @param initialBackoff   delay before the first retry
     * @param multiplier       exponential backoff factor
     * @param maxBackoff       backoff ceiling
     * @param jitter           +/- share of random jitter, so retries of a burst do not all come back together
     * @param maxSystemRetries cap on re-runs caused by recoverable system faults, after which the task is dead-lettered
     */
    public record Retry(@DefaultValue("4") int maxAttempts,
                        @DefaultValue("1s") Duration initialBackoff,
                        @DefaultValue("2.0") double multiplier,
                        @DefaultValue("60s") Duration maxBackoff,
                        @DefaultValue("0.2") double jitter,
                        @DefaultValue("10") int maxSystemRetries) {
    }

    /**
     * @param timeout                   per-attempt deadline handed to the handler
     * @param lease                     how long a claim is valid; must exceed the timeout so a live worker never loses its lease
     * @param leaseCheckInterval        how often orphaned RUNNING rows are looked for
     * @param breakerFailureThreshold   consecutive system failures that open the downstream circuit breaker
     * @param breakerOpenDuration       how long workers stop taking tasks once it opens
     */
    public record Execution(@DefaultValue("10s") Duration timeout,
                            @DefaultValue("60s") Duration lease,
                            @DefaultValue("10s") Duration leaseCheckInterval,
                            @DefaultValue("5") int breakerFailureThreshold,
                            @DefaultValue("5s") Duration breakerOpenDuration) {
    }

    /**
     * @param pollInterval how often the dispatcher checks whether the in-memory queue needs a refill
     */
    public record Dispatcher(@DefaultValue("200ms") Duration pollInterval) {
    }

    /**
     * @param awaitRunning     how long running tasks may finish on their own after a stop signal
     * @param awaitInterrupted how long interrupted tasks get to write themselves back
     */
    public record Shutdown(@DefaultValue("20s") Duration awaitRunning,
                           @DefaultValue("5s") Duration awaitInterrupted) {
    }

    /**
     * @param succeededTtl    SUCCEEDED tasks and their results are deleted after this long
     * @param cleanupInterval how often that cleanup runs
     */
    public record Retention(@DefaultValue("7d") Duration succeededTtl,
                            @DefaultValue("1h") Duration cleanupInterval) {
    }

    /**
     * The simulated model call.
     *
     * @param minLatency           shortest simulated downstream latency
     * @param maxLatency           longest simulated downstream latency
     * @param retryableFailureRate chance of a simulated rate limit or timeout (business, retryable)
     * @param systemFailureRate    chance of a simulated downstream outage (system, recoverable)
     * @param cpuWorkRounds        rounds of local hashing per KB of input, standing in for tokenisation
     */
    public record Simulation(@DefaultValue("200ms") Duration minLatency,
                             @DefaultValue("1500ms") Duration maxLatency,
                             @DefaultValue("0.08") double retryableFailureRate,
                             @DefaultValue("0.02") double systemFailureRate,
                             @DefaultValue("200") int cpuWorkRounds) {
    }
}
