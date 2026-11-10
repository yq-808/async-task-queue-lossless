-- Created on every start (CREATE ... IF NOT EXISTS), so a fresh checkout needs no manual setup.
-- Task state, task results and per-attempt history live in separate tables: state rows are
-- small and hot (dispatch and status queries), results can be large and are read once, and
-- each can be cleaned up on its own schedule.

CREATE TABLE IF NOT EXISTS task (
    id              VARCHAR(36)               NOT NULL PRIMARY KEY,
    biz_key         VARCHAR(128)              NOT NULL,
    task_type       VARCHAR(32)               NOT NULL,
    priority        TINYINT                   NOT NULL,               -- 0 = HIGH, 1 = NORMAL, 2 = LOW
    status          VARCHAR(16)               NOT NULL,               -- PENDING / RETRY_WAIT / RUNNING / SUCCEEDED / FAILED / DEAD_LETTER
    payload         VARCHAR(1048576)          NOT NULL,               -- request JSON
    payload_hash    CHAR(64)                  NOT NULL,               -- SHA-256, decides duplicate vs conflict
    attempts        INT                       NOT NULL DEFAULT 0,     -- executions started (business attempts)
    max_attempts    INT                       NOT NULL,
    system_retries  INT                       NOT NULL DEFAULT 0,     -- re-runs caused by recoverable system faults
    last_error      VARCHAR(2000),
    error_type      VARCHAR(32),
    next_run_at     TIMESTAMP WITH TIME ZONE  NOT NULL,               -- earliest dispatch time (backoff)
    lease_owner     VARCHAR(64),
    lease_until     TIMESTAMP WITH TIME ZONE,
    version         BIGINT                    NOT NULL DEFAULT 0,     -- fencing token, bumped on every claim
    queue_wait_ms   BIGINT                    NOT NULL DEFAULT 0,     -- total time spent runnable but not running
    process_ms      BIGINT                    NOT NULL DEFAULT 0,     -- total time spent running
    created_at      TIMESTAMP WITH TIME ZONE  NOT NULL,
    started_at      TIMESTAMP WITH TIME ZONE,
    finished_at     TIMESTAMP WITH TIME ZONE,
    updated_at      TIMESTAMP WITH TIME ZONE  NOT NULL,
    CONSTRAINT uk_task_biz_key UNIQUE (biz_key)
);

CREATE INDEX IF NOT EXISTS idx_task_dispatch ON task (status, priority, next_run_at);
CREATE INDEX IF NOT EXISTS idx_task_lease ON task (status, lease_until);
CREATE INDEX IF NOT EXISTS idx_task_finished ON task (status, finished_at);

CREATE TABLE IF NOT EXISTS task_result (
    task_id     VARCHAR(36)               NOT NULL PRIMARY KEY,
    result      VARCHAR(1048576)          NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE  NOT NULL
);

CREATE TABLE IF NOT EXISTS task_attempt (
    id             BIGINT AUTO_INCREMENT     NOT NULL PRIMARY KEY,
    task_id        VARCHAR(36)               NOT NULL,
    attempt_no     INT                       NOT NULL,
    outcome        VARCHAR(16)               NOT NULL,   -- SUCCEEDED / RETRY / FAILED / DEAD_LETTER / RELEASED
    error_type     VARCHAR(32),
    error_message  VARCHAR(2000),
    worker         VARCHAR(64)               NOT NULL,
    started_at     TIMESTAMP WITH TIME ZONE  NOT NULL,
    finished_at    TIMESTAMP WITH TIME ZONE  NOT NULL,
    duration_ms    BIGINT                    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_attempt_task ON task_attempt (task_id);
