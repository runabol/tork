CREATE TABLE tasks (
    id            varchar(64) not null primary key,
    created_at    timestamp   not null,
    started_at    timestamp,
    completed_at  timestamp,
    state         varchar(10) not null,
    serialized    jsonb       not null
);

CREATE INDEX idx_tasks_state ON tasks (state);

CREATE INDEX idx_tasks_serialized ON tasks USING GIN (serialized);

CREATE TABLE nodes (
    id                 varchar(64) not null primary key,
    queue              varchar(64) not null,
    started_at         timestamp   not null,
    last_heartbeat_at  timestamp   not null,
    cpu_percent        float       not null
);

CREATE INDEX idx_nodes_heartbeat ON nodes (last_heartbeat_at);

