CREATE TABLE nodes (
    id                 varchar(32) not null primary key,
    queue              varchar(64) not null,
    started_at         timestamp   not null,
    last_heartbeat_at  timestamp   not null,
    cpu_percent        float       not null
);

CREATE INDEX idx_nodes_heartbeat ON nodes (last_heartbeat_at);

CREATE TABLE jobs (
    id            varchar(32) not null primary key,
    name          varchar(256),
    state         varchar(10) not null,
    created_at    timestamp   not null,
    started_at    timestamp,
    completed_at  timestamp,
    failed_at     timestamp,
    tasks         jsonb       not null,
    position      int         not null,
    inputs        jsonb       not null,
    context       jsonb       not null
);

CREATE TABLE tasks (
    id            varchar(32) not null primary key,
    job_id        varchar(32) not null references jobs(id),
    position      int         not null,
    name          varchar(256),
    state         varchar(10) not null,
    created_at    timestamp   not null,
    scheduled_at  timestamp,
    started_at    timestamp,
    completed_at  timestamp,
    failed_at     timestamp,
    cmd           text[],
    entrypoint    text[],
    run_script    text,
    image         varchar(256),
    env           jsonb,
    queue         varchar(256),
    error_msg     text,
    pre_tasks     jsonb,
    post_tasks    jsonb,
    volumes       text[],
    node_id       varchar(32),
    retry         jsonb,
    limits        jsonb,
    timeout       varchar(8),
    result        text,
    var           varchar(16)
);

CREATE INDEX idx_tasks_state ON tasks (state);
CREATE INDEX idx_tasks_job_id ON tasks (job_id);
