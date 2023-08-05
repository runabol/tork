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
	"github.com/pkg/errors"
	"github.com/tork/task"
)

type DockerRuntime struct {
	client *client.Client
	tasks  map[string]string
}

func NewDockerRuntime() (*DockerRuntime, error) {
	dc, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	return &DockerRuntime{
		client: dc,
		tasks:  make(map[string]string),
	}, nil
}

func (d *DockerRuntime) Start(ctx context.Context, t task.Task) error {
	reader, err := d.client.ImagePull(
		ctx, t.Image, types.ImagePullOptions{})
	if err != nil {
		log.Printf("Error pulling image %s: %v\n", t.Image, err)
		return err
	}
	_, err = io.Copy(os.Stdout, reader)
	if err != nil {
		return err
	}

	rp := container.RestartPolicy{
		Name: t.RestartPolicy,
	}

	r := container.Resources{
		Memory: t.Memory,
	}

	cc := container.Config{
		Image: t.Image,
		Env:   t.Env,
		Cmd:   t.CMD,
	}

	hc := container.HostConfig{
		RestartPolicy:   rp,
		Resources:       r,
		PublishAllPorts: true,
	}

	resp, err := d.client.ContainerCreate(
		ctx, &cc, &hc, nil, nil, t.ID)
	if err != nil {
		log.Printf(
			"Error creating container using image %s: %v\n",
			t.Image, err,
		)
		return err
	}

	err = d.client.ContainerStart(
		ctx, resp.ID, types.ContainerStartOptions{})
	if err != nil {
		return errors.Wrapf(err, "error starting container %s: %v\n", resp.ID, err)
	}

	out, err := d.client.ContainerLogs(
		ctx,
		resp.ID,
		types.ContainerLogsOptions{ShowStdout: true, ShowStderr: true},
	)
	if err != nil {
		return errors.Wrapf(err, "error getting logs for container %s: %v\n", resp.ID, err)
	}

	_, err = stdcopy.StdCopy(os.Stdout, os.Stderr, out)
	if err != nil {
		return errors.Wrapf(err, "error reading the std out")
	}

	d.tasks[t.ID] = resp.ID

	return nil
}

func (d *DockerRuntime) Stop(ctx context.Context, t task.Task) error {
	containerID, ok := d.tasks[t.ID]
	if !ok {
		return nil
	}
	log.Printf("Attempting to stop container %v", containerID)
	err := d.client.ContainerStop(ctx, containerID, container.StopOptions{})
	if err != nil {
		return err
	}
	err = d.client.ContainerRemove(ctx, containerID, types.ContainerRemoveOptions{RemoveVolumes: true, RemoveLinks: false, Force: false})
	if err != nil {
		return err
	}
	return nil
}
