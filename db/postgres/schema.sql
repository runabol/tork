CREATE TABLE tasks (
    id            varchar(64) not null primary key,
    created_at    timestamp   not null,
    state         varchar(10) not null,
    serialized    jsonb       not null
);

CREATE INDEX idx_tasks_state ON tasks (state);

CREATE INDEX idx_tasks_serialized ON tasks USING GIN (serialized);
