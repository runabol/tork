# Tork

A distributed workflow engine.

**Note: This project is currently a work in progress (WIP) and is not recommended for production use.**

# Features:

- REST API
- Submit individual tasks or workflows for execution.
- Horizontally scalable
- Task isolation - tasks are executed within a container to provide isolation, idempotency, and in order to enforce resource limits
- Automatic recovery of tasks in the event of a worker crash
- Supports both stand-alone and distributed setup
- Retry failed tasks
- Pre/Post tasks
- Expression Language

# Architecture

![architecture diagram](arch.png)

**Coordinator**: responsible for managing the lifecycle of a task through its various states and for exposing a REST API to the clients.

**Worker**: responsible for executing tasks by means of a runtime (typically Docker).

**Broker**: the message-queue, pub/sub mechanism used for routing tasks.

**Datastore**: holds the state for tasks and jobs.

**Runtime**: the platform used by workers to execute tasks. Currently only Docker is supported.

# Queues

By default all tasks are routed to the `default` queue.

All workers subscribe to the `default` queue unless they make use of the `-queue` flag.

It is often desirable to route tasks to different queues in order to create specialized pools of workers.

For example, one pool of workers, specially configured to handle video transcoding can listen to video processing related tasks:

```
  go run cmd/main.go -mode standalone -queue transcoding:3 -queue default:10
```

In this example the worker would handle up to 3 transcoding-related tasks and up to 10 "regular" tasks concurrently.

This could make sense because transcoding tends to be very resource intensive so a single worker might not want to handle more than 3 concurrent tasks.

To route a task to a special queue use the `queue` property:

```
name: transcode a video
queue: transcoding
image: jrottenberg/ffmpeg:3.4-scratch
cmd:
  - -i
  - https://upload.wikimedia.org/wikipedia/commons/1/18/Big_Buck_Bunny_Trailer_1080p.ogv
  - output.mp4
```

# Getting started

## Hello World

1. Start in `standalone` mode:

```
go run cmd/main.go -mode standalone
```

2. Submit task in another terminal:

```yaml
# hello.yaml
---
name: say hello
image: ubuntu:mantic
cmd:
  - echo
  - -n
  - hello world
```

```
TASK_ID=$(curl \
  -s \
  -X POST \
  --data-binary @/tmp/task.yaml \
  -H "Content-type: text/yaml" \
  @hello.yaml \
  http://localhost:3000/task | jq -r .id)
```

Query for the status of the task:

```
# curl -s http://localhost:3000/task/$TASK_ID | jq .

{
  ...
  "state": "COMPLETED",
  "result": "hello world"
}
```

## A more interesting example

1. Download a remote video file using a `pre` task to a shared `/tmp` volume.
2. Convert the first 5 seconds of the downloaded video using `ffmpeg`.
3. Upload the converted video to a destination using a `post` task.

```yaml
# convert.yaml
---
name: convert the first 5 seconds of a video
image: jrottenberg/ffmpeg:3.4-scratch
cmd:
  - -i
  - /tmp/input.ogv
  - -t
  - "5"
  - /tmp/output.mp4
volumes:
  - /tmp
pre:
  - name: download the remote file
    image: alpine:3.18.3
    cmd:
      - wget
      - https://upload.wikimedia.org/wikipedia/commons/1/18/Big_Buck_Bunny_Trailer_1080p.ogv
      - -O
      - /tmp/input.ogv
post:
  - name: upload the converted file
    image: alpine:3.18.3
    cmd:
      - wget
      - --post-file=/tmp/output.mp4
      - https://devnull-as-a-service.com/dev/null
```

Submit the task:

```
TASK_ID=$(curl \
  -s \
  -X POST \
  --data-binary @/tmp/task.yaml \
  -H "Content-type: text/yaml" \
  @convert.yaml \
  http://localhost:3000/task | jq -r .id)
```
