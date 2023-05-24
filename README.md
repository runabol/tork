# Tork

A distributed workflow engine.

# Goals

1. Simple
2. Distributed
3. Horizontally scalable
4. Embeddable
5. Pipeline code assist
6. Pipeline validation

# Pipelines (Draft)

```yaml
inputs:
  yourName: string
    
outputs:
  yourRandomNumber: "{{randomNumber}}"

tasks:
  - type: randomInt
    startInclusive: 0
    endInclusive: 10000
    output: randomNumber
    
  - type: print            
    text: "Hello {{yourName}}"
    
  - type: sleep
    millis: "{{randomNumber}}"
    
  - label: Print a farewell
    type: print
    text: "Goodbye {{yourName}}"
```

## Special Tasks

### Map

```yaml
- type: map
  list: [
     "/path/to/file1.txt",
     "/path/to/file2.txt",
     "/path/to/file3.txt"
  ]
  mapper:
    type: fileSize         
    file: "{{item}}"
    output: fileSizes
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
