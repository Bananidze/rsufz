-- +goose Up

-- +goose StatementBegin
CREATE TABLE tasks (
    id              UUID        PRIMARY KEY,
    type            TEXT        NOT NULL,
    payload         BYTEA,
    priority        SMALLINT    NOT NULL CHECK (priority BETWEEN 0 AND 10),
    status          TEXT        NOT NULL,
    attempt_count   INT         NOT NULL DEFAULT 0,
    retry_limit     INT         NOT NULL DEFAULT 0,
    scheduled_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    worker_id       TEXT,
    last_error      TEXT,
    result          BYTEA,
    idempotency_key TEXT        UNIQUE
);
-- +goose StatementEnd

-- частичный индекс: планировщик читает только pending-задачи (МТ.5.4)
-- +goose StatementBegin
CREATE INDEX idx_tasks_status_priority_sched
    ON tasks (priority DESC, scheduled_at)
    WHERE status = 'pending';
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE task_dependencies (
    task_id    UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, depends_on)
);
-- +goose StatementEnd

-- outbox для транзакционного enqueue (Этап 6)
-- +goose StatementBegin
CREATE TABLE outbox (
    id           BIGSERIAL   PRIMARY KEY,
    aggregate_id UUID        NOT NULL,
    payload      BYTEA       NOT NULL,
    published    BOOLEAN     NOT NULL DEFAULT false,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP TABLE IF EXISTS outbox;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS task_dependencies;
-- +goose StatementEnd

-- +goose StatementBegin
DROP INDEX IF EXISTS idx_tasks_status_priority_sched;
DROP TABLE IF EXISTS tasks;
-- +goose StatementEnd
