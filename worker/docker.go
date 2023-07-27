package worker

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

type dockerClient struct {
	Client *client.Client
}

type dockerResult struct {
	Error       error
	Action      string
	ContainerID string
	Result      string
}

func (d *dockerClient) Run(cfg Config) dockerResult {
	ctx := context.Background()
	reader, err := d.Client.ImagePull(
		ctx, cfg.Image, types.ImagePullOptions{})
	if err != nil {
		log.Printf("Error pulling image %s: %v\n", cfg.Image, err)
		return dockerResult{Error: err}
	}
	io.Copy(os.Stdout, reader)

	rp := container.RestartPolicy{
		Name: cfg.RestartPolicy,
	}

	r := container.Resources{
		Memory: cfg.Memory,
	}

	cc := container.Config{
		Image: cfg.Image,
		Env:   cfg.Env,
	}

	hc := container.HostConfig{
		RestartPolicy:   rp,
		Resources:       r,
		PublishAllPorts: true,
	}

	resp, err := d.Client.ContainerCreate(
		ctx, &cc, &hc, nil, nil, cfg.Name)
	if err != nil {
		log.Printf(
			"Error creating container using image %s: %v\n",
			cfg.Image, err,
		)
		return dockerResult{Error: err}
	}

	err = d.Client.ContainerStart(
		ctx, resp.ID, types.ContainerStartOptions{})
	if err != nil {
		log.Printf("Error starting container %s: %v\n", resp.ID, err)
		return dockerResult{Error: err}
	}

	out, err := d.Client.ContainerLogs(
		ctx,
		resp.ID,
		types.ContainerLogsOptions{ShowStdout: true, ShowStderr: true},
	)
	if err != nil {
		log.Printf("Error getting logs for container %s: %v\n", resp.ID, err)
		return dockerResult{Error: err}
	}

	stdcopy.StdCopy(os.Stdout, os.Stderr, out)

	return dockerResult{
		ContainerID: resp.ID,
		Action:      "start",
		Result:      "success",
	}
}

func (d *dockerClient) Stop(id string) dockerResult {
	log.Printf("Attempting to stop container %v", id)
	ctx := context.Background()
	err := d.Client.ContainerStop(ctx, id, container.StopOptions{})
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	err = d.Client.ContainerRemove(ctx, id, types.ContainerRemoveOptions{RemoveVolumes: true, RemoveLinks: false, Force: false})
	if err != nil {
		panic(err)
	}

	return dockerResult{Action: "stop", Result: "success", Error: nil}
}
