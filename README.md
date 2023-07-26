# Tork

A distributed workflow engine.

# Goals

1. Simple
2. Lightweight
3. Embeddable
4. Horizontally scalable
5. Type-safety support for pipelines
6. Ability to execute Ad-hoc piplines
7. Easy to extend
8. No single point of failure

# Pipeline Definition (Draft)

```yaml
input:
  yourName: string
    
output:
  yourRandomNumber: "{{randomNumber}}"

tasks:
  - name: Generate a random number
    type: randomInt
    startInclusive: 0
    endInclusive: 10000
    output: randomNumber
    
  - type: print            
    text: "Hello {{yourName}}"
    
  - type: sleep
    millis: "{{randomNumber}}"
    
  - type: print
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
