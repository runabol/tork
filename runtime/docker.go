package runtime

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/tork/task"
)

type DockerRuntime struct {
	client *client.Client
}

func NewDockerRuntime() (*DockerRuntime, error) {
	dc, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	return &DockerRuntime{client: dc}, nil
}

func (d *DockerRuntime) Start(t task.Task) (string, error) {
	ctx := context.Background()
	reader, err := d.client.ImagePull(
		ctx, t.Image, types.ImagePullOptions{})
	if err != nil {
		log.Printf("Error pulling image %s: %v\n", t.Image, err)
		return "", err
	}
	io.Copy(os.Stdout, reader)

	rp := container.RestartPolicy{
		Name: t.RestartPolicy,
	}

	r := container.Resources{
		Memory: t.Memory,
	}

	cc := container.Config{
		Image: t.Image,
		Env:   t.Env,
	}

	hc := container.HostConfig{
		RestartPolicy:   rp,
		Resources:       r,
		PublishAllPorts: true,
	}

	resp, err := d.client.ContainerCreate(
		ctx, &cc, &hc, nil, nil, t.Name)
	if err != nil {
		log.Printf(
			"Error creating container using image %s: %v\n",
			t.Image, err,
		)
		return "", err
	}

	err = d.client.ContainerStart(
		ctx, resp.ID, types.ContainerStartOptions{})
	if err != nil {
		log.Printf("Error starting container %s: %v\n", resp.ID, err)
		return "", err
	}

	out, err := d.client.ContainerLogs(
		ctx,
		resp.ID,
		types.ContainerLogsOptions{ShowStdout: true, ShowStderr: true},
	)
	if err != nil {
		log.Printf("Error getting logs for container %s: %v\n", resp.ID, err)
		return "", err
	}

	stdcopy.StdCopy(os.Stdout, os.Stderr, out)

	return resp.ID, nil
}

func (d *DockerRuntime) Stop(id string) error {
	log.Printf("Attempting to stop container %v", id)
	ctx := context.Background()
	err := d.client.ContainerStop(ctx, id, container.StopOptions{})
	if err != nil {
		return err
	}
	err = d.client.ContainerRemove(ctx, id, types.ContainerRemoveOptions{RemoveVolumes: true, RemoveLinks: false, Force: false})
	if err != nil {
		return err
	}
	return nil
}
