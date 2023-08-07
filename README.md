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

# Architecture

![architecture diagram](arch.png)

**Coordinator**: responsible for managing the lifecycle of a task through its various states and for exposing a REST API to the clients.

**Worker**: responsible for executing tasks by means of a runtime (typically Docker).

**Broker**: the message-queue, pub/sub mechanism used for routing tasks.

**Datastore**: holds the state for tasks and jobs.

**Runtime**: the platform used by workers to execute tasks. Currently only Docker is supported.

# Hello World

Start in `standalone` mode:

```
go run cmd/main.go -mode standalone
```

Submit task in another terminal:

```
TASK_ID=$(curl \
  -s \
  -X POST \
  -H "content-type:application/json" \
  -d '{"image":"ubuntu:mantic","cmd":["echo","-n","hello world"]}' \
  http://localhost:3000/task | jq -r .id)
```

Query for the status of the task:

```
# curl -s http://localhost:3000/task/$TASK_ID | jq .

{
  "id": "5d16ce6055ed4e0b8084ccb55b3d7840",
  "state": "COMPLETED",
  "createdAt": "2023-08-07T18:11:00.122843-04:00",
  "scheduledAt": "2023-08-07T18:11:00.122935-04:00",
  "startedAt": "2023-08-07T18:11:00.122951-04:00",
  "completedAt": "2023-08-07T18:11:01.808909-04:00",
  "cmd": [
    "echo",
    "-n",
    "hello world"
  ],
  "image": "ubuntu:mantic",
  "result": "hello world"
}
```
