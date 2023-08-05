# Tork

A distributed workflow engine.

# Goals

1. Simple
2. Task isolation
3. Horizontally scalable
4. IDE support for pipelines (future)
5. No single point of failure
6. Automatic concurrency level on the worker side based on avaiable capacity (RAM & CPU)
7. Support multiple types of message brokers

# Sample task

```
  - name: echo your name
    queue: default
    image: ubuntu:scratch
    cmd: echo {{yourName}}
```

# Pipeline Definition (Draft)

```yaml
input:
  yourName: string

output:
  yourRandomNumber: "{{randomNumber}}"

defaults:
  image:
    amazon/aws-cli:
      env:
        AWS_ACCESS_KEY: env('DEFAULT_AWS_ACCESS_KEY')
        AWS_SECRET_KEY: env('DEFAULT_AWS_SECRET_KEY')

tasks:
  - name: echo your name
    image: ubuntu:scratch
    cmd: echo {{yourName}}

  - name: s3 cp
    image: amazon/aws-cli
    cmd: aws s3 cp s3://my-source-bucket/file s3://my-target-bucket/file

  - var: yourRandomNumber
    name: generate random number
    image: ubuntu:scratch
    cmd: echo $((RANDOM))
```

## Pre/Post Tasks

```yaml
- name: transcode a video
  image: jrottenberg/ffmpeg:3-scratch
  pre:
    - name: s3 get
      image: amazon/aws-cli
      cmd: aws s3 get s3://my-source-bucket/some-raw-video.mov /tmp/source.mov
  post:
    - name: s3 post
      image: amazon/aws-cli
      cmd: aws s3 cp my-transcode-video.mp4 s3://my-target-bucket/my-transcode-video.mp4
  cmd: ffmpeg -i /tmp/source.mov my-transcode-video.mp4
```

## Special Tasks

### Map

```yaml
- map: ["/path/to/file1.txt", "/path/to/file2.txt", "/path/to/file3.txt"]
  mapper:
    image: fileSize
    file: "{{item}}"
```

### Parallel

```yaml
- type: parallel
  tasks:
    - type: sleep
      duration: 5s

    - type: sleep
      duration: 3s
```
