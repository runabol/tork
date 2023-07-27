package worker

import (
	"context"
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

func (d *dockerClient) run(cfg Config) (string, error) {
	ctx := context.Background()
	reader, err := d.Client.ImagePull(
		ctx, cfg.Image, types.ImagePullOptions{})
	if err != nil {
		log.Printf("Error pulling image %s: %v\n", cfg.Image, err)
		return "", err
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
		return "", err
	}

	err = d.Client.ContainerStart(
		ctx, resp.ID, types.ContainerStartOptions{})
	if err != nil {
		log.Printf("Error starting container %s: %v\n", resp.ID, err)
		return "", err
	}

	out, err := d.Client.ContainerLogs(
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

func (d *dockerClient) stop(id string) error {
	log.Printf("Attempting to stop container %v", id)
	ctx := context.Background()
	err := d.Client.ContainerStop(ctx, id, container.StopOptions{})
	if err != nil {
		return err
	}
	err = d.Client.ContainerRemove(ctx, id, types.ContainerRemoveOptions{RemoveVolumes: true, RemoveLinks: false, Force: false})
	if err != nil {
		return err
	}
	return nil
}
