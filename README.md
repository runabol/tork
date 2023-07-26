# Tork

A distributed workflow engine.

# Goals

1. Simple, lightweight
2. Easy to integrate with
3. Isolated
4. Composable
5. Minimum amount of "magic"
6. Horizontally scalable
7. Type-safety support for pipelines
8. Ability to execute Ad-hoc piplines
9. No single point of failure

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
  - name: s3 cp
    image: amazon/aws-cli
    cmd: aws s3 cp s3://my-source-bucket/file s3://my-target-bucket/file
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
