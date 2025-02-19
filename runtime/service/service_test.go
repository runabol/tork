package service

import (
	"context"
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/runtime/docker"
	"github.com/stretchr/testify/assert"
)

func Test_reservePort(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	p := NewServiceRuntime(rt)

	port, err := p.reservePort()
	assert.NoError(t, err)
	assert.NotEmpty(t, port)
	assert.Contains(t, p.usedPorts, port)
	p.releasePort(port)
	assert.NotContains(t, p.usedPorts, port)
	p.releasePort(port)
}

func Test_Run(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	p := NewServiceRuntime(rt)

	ctx := context.Background()
	task := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "node:lts-alpine3.20",
		Run:   "node server.js",
		Files: map[string]string{
			"server.js": `
             const http = require('http');
             const hostname = '0.0.0.0';
             const port = 8080;
             const server = http.createServer((req, res) => {
               res.statusCode = 200;
               res.setHeader('Content-Type', 'text/plain');
               res.end('Hello World\n');
             });
            server.listen(port, hostname, () => {
              console.log('server running');
            });
			`,
		},
		Service: &tork.Service{
			Name: "hello-service",
			Port: "8080",
			Path: "/test",
		},
	}

	err = p.Run(ctx, task)
	assert.NoError(t, err)
	assert.Equal(t, "Hello World\n", task.Result)

	err = p.Shutdown(ctx)
	assert.NoError(t, err)
}

func Test_Run_Concurrent(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	p := NewServiceRuntime(rt)

	ctx := context.Background()
	task := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "node:lts-alpine3.20",
		Run:   "node server.js",
		Files: map[string]string{
			"server.js": `
			 const http = require('http');
			 const hostname = '0.0.0.0';
			 const port = 8080;
			 const server = http.createServer((req, res) => {
			   res.statusCode = 200;
			   res.setHeader('Content-Type', 'text/plain');
			   res.end('Hello World\n');
			 });
			server.listen(port, hostname, () => {
			  console.log('server running');
			});
			`,
		},
		Service: &tork.Service{
			Name: "hello-service",
			Port: "8080",
			Path: "/test1",
		},
	}

	errCh := make(chan error, 2)

	go func() {
		errCh <- p.Run(ctx, task)
	}()

	go func() {
		errCh <- p.Run(ctx, task)
	}()

	for i := 0; i < 2; i++ {
		err := <-errCh
		assert.NoError(t, err)
	}

	assert.Equal(t, "Hello World\n", task.Result)
	assert.Equal(t, "Hello World\n", task.Result)

	err = p.Shutdown(ctx)
	assert.NoError(t, err)
}

func Test_Run_ServiceFieldRequired(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	p := NewServiceRuntime(rt)

	ctx := context.Background()
	task := &tork.Task{}

	err = p.Run(ctx, task)
	assert.Error(t, err)
	assert.Equal(t, "task service field is required", err.Error())
}
