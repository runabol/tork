## Tork

[![Go Report Card](https://goreportcard.com/badge/github.com/runabol/tork)](https://goreportcard.com/report/github.com/runabol/tork)
![Build Status](https://github.com/runabol/tork/workflows/ci/badge.svg)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)

A Golang based high-performance, scalable and distributed workflow engine.

## Features:

- [REST API](https://www.tork.run/rest)
- [Extensible](https://www.tork.run/customize)
- Horizontally scalable
- Task isolation - tasks are executed within a container to provide isolation, idempotency, and in order to enforce resource [limits](#limits)
- Automatic recovery of tasks in the event of a worker crash
- Supports both stand-alone and [distributed](https://www.tork.run/installation#running-in-a-distributed-mode) setup
- Retry failed tasks
- [Pre/Post tasks](https://www.tork.run/tasks#pre-post-tasks)
- No single point of failure
- Task timeout
- [Full-text search](https://www.tork.run/rest#list-jobs)
- [Expression Language](https://www.tork.run/tasks#expressions)
- [Conditional Tasks](https://www.tork.run/tasks#expressions)
- [Parallel Task](https://www.tork.run/tasks#parallel-task)
- [Each Task](https://www.tork.run/tasks#each-task)
- [Subjob Task](https://www.tork.run/tasks#sub-job-task)
- [Web UI](#web-ui)

## Documentation

See [https://www.tork.run](https://tork.run) for the full documentation.

## Quick Start

1. Ensure you have [Docker](https://docs.docker.com/get-docker/) with API Version >= 1.42 (use `docker version | grep API` to check).

2. Download the binary for your system from the [releases](https://github.com/runabol/tork/releases/latest) page.

### Hello World

Start in `standalone` mode:

```
./tork run standalone
```

Submit a job in another terminal:

```yaml
# hello.yaml
---
name: hello job
tasks:
  - name: say hello
    image: ubuntu:mantic #docker image
    run: |
      echo -n hello world
  - name: say goodbye
    image: ubuntu:mantic
    run: |
      echo -n bye world
```

```bash
JOB_ID=$(curl \
  -s \
  -X POST \
  --data-binary @hello.yaml \
  -H "Content-type: text/yaml" \
  http://localhost:8000/jobs | jq -r .id)
```

Query for the status of the job:

```bash
curl -s http://localhost:8000/jobs/$JOB_ID | jq .

{
  "id": "ed0dba93d262492b8cf26e6c1c4f1c98",
  "state": "COMPLETED",
  ...
  "execution": [
    {
      ...
      "state": "COMPLETED",
    }
  ],
}
```

### A slightly more interesting example

The following job:

1. Downloads a remote video file using a `pre` task to a shared `/tmp` volume.
2. Converts the first 5 seconds of the downloaded video using `ffmpeg`.
3. Uploads the converted video to a destination using a `post` task.

```yaml
# convert.yaml
---
name: convert a video
inputs:
  source: https://upload.wikimedia.org/wikipedia/commons/1/18/Big_Buck_Bunny_Trailer_1080p.ogv
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
        env:
          SOURCE_URL: "{{ inputs.source }}"
        run: |
          wget \
          $SOURCE_URL \
          -O /tmp/input.ogv
    post:
      - name: upload the converted file
        image: alpine:3.18.3
        run: |
          wget \
          --post-file=/tmp/output.mp4 \
          https://devnull-as-a-service.com/dev/null
```

Submit the job in another terminal:

```
JOB_ID=$(curl \
  -s \
  -X POST \
  --data-binary @convert.yaml \
  -H "Content-type: text/yaml" \
  http://localhost:8000/jobs | jq -r .id)
```

### More examples

Check out the [examples](examples/) folder.

## REST API

See the [REST API](https://www.tork.run/rest) documentation.

### Swagger Docs

Start Tork in `standalone` or `coordinator` mode.

```shell
go run cmd/main.go run standalone
```

Visit [http://localhost:8000/swagger](http://localhost:8000/swagger)

## Web UI

[Tork Web](https://www.tork.run/web-ui) is a web based tool for interacting with Tork.

## License

Copyright (c) 2023-present Arik Cohen. Tork is free and open-source software licensed under the MIT License.
