// Package swagger Code generated by swaggo/swag. DO NOT EDIT
package swagger

import "github.com/swaggo/swag"

const docTemplate = `{
    "schemes": {{ marshal .Schemes }},
    "swagger": "2.0",
    "info": {
        "description": "{{escape .Description}}",
        "title": "{{.Title}}",
        "contact": {
            "name": "Arik Cohen",
            "url": "https://tork.run",
            "email": "contact@tork.run"
        },
        "license": {
            "name": "MIT",
            "url": "https://github.com/runabol/tork/blob/main/LICENSE"
        },
        "version": "{{.Version}}"
    },
    "host": "{{.Host}}",
    "basePath": "{{.BasePath}}",
    "paths": {
        "/health": {
            "get": {
                "description": "get the status of server.",
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "management"
                ],
                "summary": "Shows application health information.",
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/api.HealthResponse"
                        }
                    }
                }
            }
        },
        "/jobs": {
            "get": {
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "jobs"
                ],
                "summary": "Show a list of jobs",
                "parameters": [
                    {
                        "type": "string",
                        "description": "search string",
                        "name": "q",
                        "in": "query"
                    },
                    {
                        "type": "integer",
                        "description": "page number",
                        "name": "page",
                        "in": "query"
                    },
                    {
                        "type": "integer",
                        "description": "page size",
                        "name": "size",
                        "in": "query"
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "type": "array",
                            "items": {
                                "$ref": "#/definitions/tork.JobSummary"
                            }
                        }
                    }
                }
            },
            "post": {
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "jobs"
                ],
                "summary": "Create a new job",
                "parameters": [
                    {
                        "description": "body",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/input.Job"
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/tork.JobSummary"
                        }
                    }
                }
            }
        },
        "/jobs/{id}": {
            "get": {
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "jobs"
                ],
                "summary": "Get a job by id",
                "parameters": [
                    {
                        "type": "string",
                        "description": "Job ID",
                        "name": "id",
                        "in": "path",
                        "required": true
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/tork.Job"
                        }
                    },
                    "404": {
                        "description": "Not Found",
                        "schema": {
                            "$ref": "#/definitions/echo.HTTPError"
                        }
                    }
                }
            }
        },
        "/jobs/{id}/cancel": {
            "put": {
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "jobs"
                ],
                "summary": "Cancel a running job",
                "parameters": [
                    {
                        "type": "string",
                        "description": "Job ID",
                        "name": "id",
                        "in": "path",
                        "required": true
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "400": {
                        "description": "Bad Request",
                        "schema": {
                            "$ref": "#/definitions/echo.HTTPError"
                        }
                    },
                    "404": {
                        "description": "Not Found",
                        "schema": {
                            "$ref": "#/definitions/echo.HTTPError"
                        }
                    }
                }
            }
        },
        "/jobs/{id}/restart": {
            "put": {
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "jobs"
                ],
                "summary": "Restart a cancelled/failed job",
                "parameters": [
                    {
                        "type": "string",
                        "description": "Job ID",
                        "name": "id",
                        "in": "path",
                        "required": true
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "type": "string"
                        }
                    },
                    "400": {
                        "description": "Bad Request",
                        "schema": {
                            "$ref": "#/definitions/echo.HTTPError"
                        }
                    },
                    "404": {
                        "description": "Not Found",
                        "schema": {
                            "$ref": "#/definitions/echo.HTTPError"
                        }
                    }
                }
            }
        },
        "/nodes": {
            "get": {
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "nodes"
                ],
                "summary": "Get a list of active worker nodes",
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "type": "array",
                            "items": {
                                "$ref": "#/definitions/tork.Node"
                            }
                        }
                    }
                }
            }
        },
        "/queues": {
            "get": {
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "queues"
                ],
                "summary": "get a list of queues",
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "type": "array",
                            "items": {
                                "$ref": "#/definitions/mq.QueueInfo"
                            }
                        }
                    }
                }
            }
        },
        "/tasks/{id}": {
            "get": {
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "tasks"
                ],
                "summary": "Get a task by id",
                "parameters": [
                    {
                        "type": "string",
                        "description": "Task ID",
                        "name": "id",
                        "in": "path",
                        "required": true
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/tork.Task"
                        }
                    }
                }
            }
        }
    },
    "definitions": {
        "api.HealthResponse": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string"
                }
            }
        },
        "echo.HTTPError": {
            "type": "object",
            "properties": {
                "message": {}
            }
        },
        "input.AuxTask": {
            "type": "object",
            "required": [
                "image",
                "name"
            ],
            "properties": {
                "cmd": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "description": {
                    "type": "string"
                },
                "entrypoint": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "env": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "string"
                    }
                },
                "image": {
                    "type": "string"
                },
                "name": {
                    "type": "string"
                },
                "run": {
                    "type": "string"
                },
                "timeout": {
                    "type": "string"
                }
            }
        },
        "input.Each": {
            "type": "object",
            "required": [
                "list",
                "task"
            ],
            "properties": {
                "list": {
                    "type": "string"
                },
                "task": {
                    "$ref": "#/definitions/input.Task"
                }
            }
        },
        "input.Job": {
            "type": "object",
            "required": [
                "name",
                "tasks"
            ],
            "properties": {
                "description": {
                    "type": "string"
                },
                "inputs": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "string"
                    }
                },
                "name": {
                    "type": "string"
                },
                "output": {
                    "type": "string"
                },
                "tasks": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "$ref": "#/definitions/input.Task"
                    }
                }
            }
        },
        "input.Limits": {
            "type": "object",
            "properties": {
                "cpus": {
                    "type": "string"
                },
                "memory": {
                    "type": "string"
                }
            }
        },
        "input.Parallel": {
            "type": "object",
            "required": [
                "tasks"
            ],
            "properties": {
                "tasks": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "$ref": "#/definitions/input.Task"
                    }
                }
            }
        },
        "input.Retry": {
            "type": "object",
            "required": [
                "limit"
            ],
            "properties": {
                "limit": {
                    "type": "integer",
                    "maximum": 10,
                    "minimum": 1
                }
            }
        },
        "input.SubJob": {
            "type": "object",
            "required": [
                "name",
                "tasks"
            ],
            "properties": {
                "description": {
                    "type": "string"
                },
                "id": {
                    "type": "string"
                },
                "inputs": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "string"
                    }
                },
                "name": {
                    "type": "string"
                },
                "output": {
                    "type": "string"
                },
                "tasks": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/input.Task"
                    }
                }
            }
        },
        "input.Task": {
            "type": "object",
            "required": [
                "name"
            ],
            "properties": {
                "cmd": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "description": {
                    "type": "string"
                },
                "each": {
                    "$ref": "#/definitions/input.Each"
                },
                "entrypoint": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "env": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "string"
                    }
                },
                "files": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "string"
                    }
                },
                "if": {
                    "type": "string"
                },
                "image": {
                    "type": "string"
                },
                "limits": {
                    "$ref": "#/definitions/input.Limits"
                },
                "name": {
                    "type": "string"
                },
                "networks": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "parallel": {
                    "$ref": "#/definitions/input.Parallel"
                },
                "post": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/input.AuxTask"
                    }
                },
                "pre": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/input.AuxTask"
                    }
                },
                "queue": {
                    "type": "string"
                },
                "retry": {
                    "$ref": "#/definitions/input.Retry"
                },
                "run": {
                    "type": "string"
                },
                "subjob": {
                    "$ref": "#/definitions/input.SubJob"
                },
                "timeout": {
                    "type": "string"
                },
                "var": {
                    "type": "string"
                },
                "volumes": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                }
            }
        },
        "mq.QueueInfo": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string"
                },
                "size": {
                    "type": "integer"
                },
                "subscribers": {
                    "type": "integer"
                },
                "unacked": {
                    "type": "integer"
                }
            }
        },
        "tork.EachTask": {
            "type": "object",
            "properties": {
                "completions": {
                    "type": "integer"
                },
                "list": {
                    "type": "string"
                },
                "size": {
                    "type": "integer"
                },
                "task": {
                    "$ref": "#/definitions/tork.Task"
                }
            }
        },
        "tork.Job": {
            "type": "object",
            "properties": {
                "completedAt": {
                    "type": "string"
                },
                "context": {
                    "$ref": "#/definitions/tork.JobContext"
                },
                "createdAt": {
                    "type": "string"
                },
                "description": {
                    "type": "string"
                },
                "error": {
                    "type": "string"
                },
                "execution": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/tork.Task"
                    }
                },
                "failedAt": {
                    "type": "string"
                },
                "id": {
                    "type": "string"
                },
                "inputs": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "string"
                    }
                },
                "name": {
                    "type": "string"
                },
                "output": {
                    "type": "string"
                },
                "parentId": {
                    "type": "string"
                },
                "position": {
                    "type": "integer"
                },
                "result": {
                    "type": "string"
                },
                "startedAt": {
                    "type": "string"
                },
                "state": {
                    "$ref": "#/definitions/tork.JobState"
                },
                "taskCount": {
                    "type": "integer"
                },
                "tasks": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/tork.Task"
                    }
                }
            }
        },
        "tork.JobContext": {
            "type": "object",
            "properties": {
                "inputs": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "string"
                    }
                },
                "tasks": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "string"
                    }
                }
            }
        },
        "tork.JobState": {
            "type": "string",
            "enum": [
                "PENDING",
                "RUNNING",
                "CANCELLED",
                "COMPLETED",
                "FAILED",
                "RESTART"
            ],
            "x-enum-varnames": [
                "JobStatePending",
                "JobStateRunning",
                "JobStateCancelled",
                "JobStateCompleted",
                "JobStateFailed",
                "JobStateRestart"
            ]
        },
        "tork.JobSummary": {
            "type": "object",
            "properties": {
                "completedAt": {
                    "type": "string"
                },
                "createdAt": {
                    "type": "string"
                },
                "description": {
                    "type": "string"
                },
                "error": {
                    "type": "string"
                },
                "failedAt": {
                    "type": "string"
                },
                "id": {
                    "type": "string"
                },
                "name": {
                    "type": "string"
                },
                "output": {
                    "type": "string"
                },
                "parentId": {
                    "type": "string"
                },
                "position": {
                    "type": "integer"
                },
                "result": {
                    "type": "string"
                },
                "startedAt": {
                    "type": "string"
                },
                "state": {
                    "$ref": "#/definitions/tork.JobState"
                },
                "taskCount": {
                    "type": "integer"
                }
            }
        },
        "tork.Node": {
            "type": "object",
            "properties": {
                "cpuPercent": {
                    "type": "number"
                },
                "hostname": {
                    "type": "string"
                },
                "id": {
                    "type": "string"
                },
                "lastHeartbeatAt": {
                    "type": "string"
                },
                "queue": {
                    "type": "string"
                },
                "startedAt": {
                    "type": "string"
                },
                "status": {
                    "$ref": "#/definitions/tork.NodeStatus"
                },
                "taskCount": {
                    "type": "integer"
                },
                "version": {
                    "type": "string"
                }
            }
        },
        "tork.NodeStatus": {
            "type": "string",
            "enum": [
                "UP",
                "DOWN",
                "OFFLINE"
            ],
            "x-enum-varnames": [
                "NodeStatusUP",
                "NodeStatusDown",
                "NodeStatusOffline"
            ]
        },
        "tork.ParallelTask": {
            "type": "object",
            "properties": {
                "completions": {
                    "type": "integer"
                },
                "tasks": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/tork.Task"
                    }
                }
            }
        },
        "tork.SubJobTask": {
            "type": "object",
            "properties": {
                "description": {
                    "type": "string"
                },
                "id": {
                    "type": "string"
                },
                "inputs": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "string"
                    }
                },
                "name": {
                    "type": "string"
                },
                "output": {
                    "type": "string"
                },
                "tasks": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/tork.Task"
                    }
                }
            }
        },
        "tork.Task": {
            "type": "object",
            "properties": {
                "cmd": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "completedAt": {
                    "type": "string"
                },
                "createdAt": {
                    "type": "string"
                },
                "description": {
                    "type": "string"
                },
                "each": {
                    "$ref": "#/definitions/tork.EachTask"
                },
                "entrypoint": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "env": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "string"
                    }
                },
                "error": {
                    "type": "string"
                },
                "failedAt": {
                    "type": "string"
                },
                "files": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "string"
                    }
                },
                "id": {
                    "type": "string"
                },
                "if": {
                    "type": "string"
                },
                "image": {
                    "type": "string"
                },
                "jobId": {
                    "type": "string"
                },
                "limits": {
                    "$ref": "#/definitions/tork.TaskLimits"
                },
                "name": {
                    "type": "string"
                },
                "networks": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "nodeId": {
                    "type": "string"
                },
                "parallel": {
                    "$ref": "#/definitions/tork.ParallelTask"
                },
                "parentId": {
                    "type": "string"
                },
                "position": {
                    "type": "integer"
                },
                "post": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/tork.Task"
                    }
                },
                "pre": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/tork.Task"
                    }
                },
                "queue": {
                    "type": "string"
                },
                "result": {
                    "type": "string"
                },
                "retry": {
                    "$ref": "#/definitions/tork.TaskRetry"
                },
                "run": {
                    "type": "string"
                },
                "scheduledAt": {
                    "type": "string"
                },
                "startedAt": {
                    "type": "string"
                },
                "state": {
                    "$ref": "#/definitions/tork.TaskState"
                },
                "subjob": {
                    "$ref": "#/definitions/tork.SubJobTask"
                },
                "timeout": {
                    "type": "string"
                },
                "var": {
                    "type": "string"
                },
                "volumes": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                }
            }
        },
        "tork.TaskLimits": {
            "type": "object",
            "properties": {
                "cpus": {
                    "type": "string"
                },
                "memory": {
                    "type": "string"
                }
            }
        },
        "tork.TaskRetry": {
            "type": "object",
            "properties": {
                "attempts": {
                    "type": "integer"
                },
                "limit": {
                    "type": "integer"
                }
            }
        },
        "tork.TaskState": {
            "type": "string",
            "enum": [
                "PENDING",
                "SCHEDULED",
                "RUNNING",
                "CANCELLED",
                "STOPPED",
                "COMPLETED",
                "FAILED"
            ],
            "x-enum-varnames": [
                "TaskStatePending",
                "TaskStateScheduled",
                "TaskStateRunning",
                "TaskStateCancelled",
                "TaskStateStopped",
                "TaskStateCompleted",
                "TaskStateFailed"
            ]
        }
    }
}`

// SwaggerInfo holds exported Swagger Info so clients can modify it
var SwaggerInfo = &swag.Spec{
	Version:          "1.0",
	Host:             "localhost:8000",
	BasePath:         "/",
	Schemes:          []string{"http"},
	Title:            "Tork API",
	Description:      "",
	InfoInstanceName: "swagger",
	SwaggerTemplate:  docTemplate,
	LeftDelim:        "{{",
	RightDelim:       "}}",
}

func init() {
	swag.Register(SwaggerInfo.InstanceName(), SwaggerInfo)
}
