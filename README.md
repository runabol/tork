# Tork

A Golang based high-performance, scalable and distributed job execution engine.

# Features:

- [REST API](#rest-api)
- Horizontally scalable
- Task isolation - tasks are executed within a container to provide isolation, idempotency, and in order to enforce resource [limits](#limits)
- Automatic recovery of tasks in the event of a worker crash
- Supports both stand-alone and [distributed](#running-in-a-distributed-mode) setup
- Retry failed tasks
- [Pre/Post tasks](#a-slightly-more-interesting-example)
- No single point of failure.
- Task timeout

# Architecture

![architecture diagram](arch.png)

**Coordinator**: responsible for managing the lifecycle of a task through its various states and for exposing a REST API to the clients.

**Worker**: responsible for executing tasks by means of a runtime (typically Docker).

**Broker**: the message-queue, pub/sub mechanism used for routing tasks.

**Datastore**: holds the state for tasks and jobs.

**Runtime**: the platform used by workers to execute tasks. Currently only Docker is supported.

# Getting started

## Prerequisites

Before running the examples, ensure you have the following software installed:

- [Go](https://golang.org/doc/install) 1.18 or later
- [Docker](https://docs.docker.com/get-docker/) with API Version >= 1.42 (use `docker version | grep API` to check)

## Hello World

Start in `standalone` mode:

```
go run cmd/main.go -mode standalone
```

Submit a job in another terminal:

```yaml
# hello.yaml
---
name: hello job
tasks:
  - name: say hello
    image: ubuntu:mantic
    run: |
      echo -n hello world
```

```bash
JOB_ID=$(curl \
  -s \
  -X POST \
  --data-binary @hello.yaml \
  -H "Content-type: text/yaml" \
  http://localhost:3000/job | jq -r .id)
```

Query for the status of the task:

```bash
curl -s http://localhost:3000/job/$JOB_ID | jq .

{
  "id": "ed0dba93d262492b8cf26e6c1c4f1c98",
  "state": "COMPLETED",
  ...
  "execution": [
    {
      ...
      "state": "COMPLETED",
      "result": "hello world",
    }
  ],
}
```

## A slightly more interesting example

The following job:

1. Downloads a remote video file using a `pre` task to a shared `/tmp` volume.
2. Converts the first 5 seconds of the downloaded video using `ffmpeg`.
3. Uploads the converted video to a destination using a `post` task.

```yaml
# convert.yaml
---
name: convert a video
tasks:
  - name: convert the first 5 seconds of a video
    image: jrottenberg/ffmpeg:3.4-alpine
    run: |
      ffmpeg -i /tmp/input.ogv -t 5 /tmp/output.mp4
    volumes:
      - /tmp
    pre:
      - name: download the remote file
        image: alpine:3.18.3
        run: |
          wget \
          https://upload.wikimedia.org/wikipedia/commons/1/18/Big_Buck_Bunny_Trailer_1080p.ogv \
          -O /tmp/input.ogv
    post:
      - name: upload the converted file
        image: alpine:3.18.3
        run: |
          wget \
          --post-file=/tmp/output.mp4 \
          https://devnull-as-a-service.com/dev/null
```

# Queues

By default all tasks are routed to the `default` queue.

All workers subscribe to the `default` queue unless they make use of the `-queue` flag.

It is often desirable to route tasks to different queues in order to create specialized pools of workers.

For example, one pool of workers, specially configured to handle video transcoding can listen to video processing related tasks:

```
go run cmd/main.go -mode worker -queue transcoding:3 -queue default:10
```

In this example the worker would handle up to 3 transcoding-related tasks and up to 10 "regular" tasks concurrently.

This could make sense because transcoding tends to be very resource intensive so a single worker might not want to handle more than 3 concurrent tasks.

To route a task to a special queue use the `queue` property:

```yaml
name: transcode a video
queue: transcoding
image: jrottenberg/ffmpeg:3.4-alpine
run: |
  ffmpeg \
    -i https://upload.wikimedia.org/wikipedia/commons/1/18/Big_Buck_Bunny_Trailer_1080p.ogv \
    output.mp4
```

## Special queues

- `jobs` - incoming jobs land in this queue prior to being scheduled for processing by the Coordinator.

- `pending` - incoming tasks land in this queue prior to being scheduled for processing by the Coordinator.

- `started` - when the worker starts working on a task it inserts the task to this queue to notify the Coordinator.

- `completed` - when a worker completes the processing of a task successfully it inserts it -- along with its output -- to this queue to notify the Coordinator.

- `error` - when a worker encounters an error while processing a task it inserts the task to this queue to notify the Coordinator.

- `hearbeat` - the queue used by workers to periodically notify the Coordinator about their "aliveness".

- `x-<worker id>` - each worker subscribes to an exclusive queue which can be used by the coordinator to cancel tasks started by a particular worker.

# Environment Variables

You can set custom environment variables for a given task by using the `env` property:

```yaml
name: print a message
image: ubuntu:mantic
env:
  INTRO: hello world
  OUTRO: bye world
run: |
  echo $INTRO
  echo $OUTRO
```

## Secrets

By convention, any environment variables which contain the keywords `SECRET`, `PASSWORD` or `ACCESS_KEY` in their names will have their values automatically redacted from logs as well as from API responses.

**Warning**: Tork automatically redacts secrets printed to the log, but you should avoid printing secrets to the log intentionally.

# Datastore

The `Datastore` is responsible for holding job and task metadata.

You can specify which type of datastore to use using the `-datastore` flag.

`inmemory`: the default implementation. Typically isn't suitable for production because all state will be lost upon restart.

`postgres`: uses a [Postgres](https://www.postgresql.org) database as the underlying implementation. Example:

Start Postgres DB:

```bash
docker compose up postgres -d
```

Run a migration to create the database schema

```bash
docker compose up migration
```

Start Tork

```bash
go run cmd/main.go \
  -mode standalone \
  -datastore postgres \
  -postgres-dsn "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
```

# Running in a distributed mode

To run in distributed mode we need to use an external message broker.

Start RabbitMQ:

```bash
docker compose up rabbitmq -d
```

Start the Coordinator:

```bash
go run cmd/main.go \
 -mode coordinator \
 -broker rabbitmq \
 -rabbitmq-url amqp://guest:guest@localhost:5672
```

Start the worker(s):

```bash
go run cmd/main.go \
 -mode worker \
 -broker rabbitmq \
 -rabbitmq-url amqp://guest:guest@localhost:5672
```

# Limits

By default, a task has no resource constraints and can use as much of a given resource as the host’s kernel scheduler allows.

Tork provides several ways to control how much CPU and Memory a task can use:

**Setting global defaults at the worker level**

`--default-cpus-limit` - the default maximum number of CPUs that a task is allowed to use when executing on as given worker. For instance, if the host machine has two CPUs and you set `--default-cpus-limit="1.5"`, a task is guaranteed at most one and a half of the CPUs.

`--default-memory-limit` - the default maximum amount of memory that a task is allowed to use when executing on a given worker. Units are specified as a positive integer, followed by a suffix of `b`, `k`, `m`, `g`, to indicate bytes, kilobytes, megabytes, or gigabytes.

**Setting limits on the task itself**

For more fine-grained control, default limits can be overridden at an individual task level:

```yaml
name: some task
image: ubuntu:mantic
run: sleep 10
limits:
  cpus: .5
  memory: 10m
```

# REST API

## Submit a job

Submit a new job to be scheduled for execution

**Path:**

```
POST /job
```

**Headers:**

JSON Input:

```
Content-Type:application/json
```

YAML Input:

```
Content-Type:text/yaml
```

**Request body:**

- `name` - a human-readable name for the job
- `tasks` - the list of task to execute

task properties:

- `name` - a human-readable name for the task
- `image` (required) - the docker image to use to execute the task
- `run` - the script to run on the container
- `env` - a key-value map of environment variables
- `queue` - the name of the queue that the task should be routed to. See [queues](#queues).
- `pre` - the list of tasks to execute prior to executing the actual task.
- `post` - the list of tasks to execute post execution of the actual task.
- `volumes` - a list of temporary volumes, created for the duration of the execution of the task. Useful for sharing state between the task
  and its `pre` and `post` tasks.
  ```yaml
  volumes:
    - /data1
    - /data2
  ```
  **note**: if you get an `invalid mount config for type "bind": bind source path does not exist` error it's most likely due to the fact that the Docker daemon isn't allowed to mount volumes from your default `$TMPDIR`. Try using the `--temp-dir` flag to explictly set it to another directory.
- `retry` - the retry configuration to execute in case of a failure. Example:
  ```yaml
  retry:
    limit: 5 # will retry up to 5 times
    initialDelay: 5s # optional: default 1s (max: 5m)
    scalingFactor: # optional: default 2 (max: 10)
  ```
- `timeout` - the amount of time (specified as `300ms` or `1h` or `45m` etc.) that a task may execute before it is cancelled.

**Examples:**

JSON:

```bash
curl -X POST "http://localhost:3000/job" \
     -H "Content-Type: application/json" \
     -d '{"name":"sample job","tasks":[{
       "name": "sample task",
       "image": "ubuntu:mantic",
       "run": "echo hello world"
     }]}'
```

YAML:

```bash
curl -X POST "http://localhost:3000/job" \
     -H "Content-Type: text/yaml" \
     -d \
'
name: sample job
tasks:
  - name: sample task
    image: ubuntu:mantic,
    run: echo hello world
'
```

**Response:**

```
HTTP 200

{
  "id": "68c602bed6d34d7f9130bfa13786e422",
  "name": "sample job",
  "state": "PENDING",
  "createdAt": "2023-08-12T15:55:12.143998-04:00",
  "tasks": [{
    "name": "sample task",
    "run": "echo hello world",
    "image": "ubuntu:mantic,"
  }]
}
```

## Cancel a running task

**Path:**

```
PUT /task/{task id}/cancel
```

**Response:**

Success:

```
HTTP 200

{
  "status": "OK"
}
```

Failure:

```
400 Bad Request

{
  "error": "task in not running"
}
```

# License

Copyright (c) 2023-present Arik Cohen. Tork is free and open-source software licensed under the MIT License.
