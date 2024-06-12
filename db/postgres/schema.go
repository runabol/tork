package postgres

const SCHEMA = `
CREATE TABLE nodes (
    id                 varchar(32)  not null primary key,
    name               varchar(64)  not null,
    queue              varchar(64)  not null,
    started_at         timestamp    not null,
    last_heartbeat_at  timestamp    not null,
    cpu_percent        float        not null,
    status             varchar(10)  not null,
    hostname           varchar(128) not null,
    task_count         int          not null,
    version_           varchar(32)  not null
);

CREATE INDEX idx_nodes_heartbeat ON nodes (last_heartbeat_at);

CREATE TABLE users (
    id          varchar(32)  not null primary key,
    name        varchar(64)  not null,
    username_   varchar(64)  not null unique,
    password_   varchar(256) not null,
    created_at  timestamp    not null,
    is_disabled boolean      not null default false
);

insert into users (id,name,username_,password_,created_at,is_disabled) (SELECT REPLACE(gen_random_uuid()::text, '-', ''),'Guest','guest','',current_timestamp,true);

CREATE TABLE roles (
    id          varchar(32)  not null primary key,
    name        varchar(64)  not null,
    slug        varchar(64)  not null unique,
    created_at  timestamp    not null
);

CREATE UNIQUE INDEX idx_roles_slug ON roles (slug);

insert into roles (id,name,slug,created_at) (SELECT REPLACE(gen_random_uuid()::text, '-', ''),'Public','public',current_timestamp);

CREATE TABLE users_roles (
    id         varchar(32) not null primary key,
    user_id    varchar(32) not null references users(id),
    role_id    varchar(32) not null references roles(id),
    created_at timestamp   not null
);

CREATE UNIQUE INDEX idx_users_roles_uniq ON users_roles (user_id,role_id);

CREATE TABLE jobs (
    id            varchar(32) not null primary key,
    name          varchar(256),
    tags          text[]      not null default '{}',
    state         varchar(10) not null,
    created_at    timestamp   not null,
	created_by    varchar(32) not null references users(id),
    started_at    timestamp,
    completed_at  timestamp,
	delete_at     timestamp,
    failed_at     timestamp,
    tasks         jsonb       not null,
    position      int         not null,
    inputs        jsonb       not null,
    context       jsonb       not null,
    description   text,
    parent_id     varchar(32),
    task_count    int         not null,
    output_       text,
    result        text,
    error_        text,
    defaults      jsonb,
    webhooks      jsonb,
	auto_delete   jsonb
);

CREATE INDEX idx_jobs_state ON jobs (state);


CREATE INDEX idx_jobs_created_at ON jobs (created_at);

ALTER TABLE jobs ADD COLUMN ts tsvector NOT NULL
    GENERATED ALWAYS AS (
        setweight(to_tsvector('english',description),'C')  ||  
        setweight(to_tsvector('english',name),'B') ||
        setweight(to_tsvector('english',state),'A') 
    ) STORED;

CREATE INDEX jobs_ts_idx ON jobs USING GIN (ts);

create index jobs_tags_idx on jobs using gin (tags);

CREATE TABLE jobs_perms (
    id      varchar(32) not null primary key,
    job_id  varchar(32) not null references jobs(id),
    user_id varchar(32)          references users(id),
    role_id varchar(32)          references roles(id)
);

CREATE INDEX jobs_perms_job_id_idx ON jobs_perms (job_id);
CREATE INDEX jobs_perms_user_role_idx ON jobs_perms (user_id,role_id);

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
    registry      jsonb,
    env           jsonb,
    files_        jsonb,
    queue         varchar(256),
    error_        text,
    pre_tasks     jsonb,
    post_tasks    jsonb,
    mounts        jsonb,
    node_id       varchar(32),
    retry         jsonb,
    limits        jsonb,
    timeout       varchar(8),
    result        text,
    var           varchar(64),
    parallel      jsonb,
    parent_id     varchar(32),
    each_         jsonb,
    description   text,
    subjob        jsonb,
    networks      text[],
    gpus          text,
    if_           text,
    tags          text[],
    priority      int,
    workdir       varchar(256)
);

CREATE INDEX idx_tasks_state ON tasks (state);
CREATE INDEX idx_tasks_job_id ON tasks (job_id);

CREATE TABLE tasks_log_parts (
    id         varchar(32) not null primary key,
    number_    int         not null,
    task_id    varchar(32) not null references tasks(id),
    created_at timestamp   not null,
    contents   text        not null
);

CREATE INDEX idx_tasks_log_parts_task_id ON tasks_log_parts (task_id);
CREATE INDEX idx_tasks_log_parts_created_at ON tasks_log_parts (created_at);
`
