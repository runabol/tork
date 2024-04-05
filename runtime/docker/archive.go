package docker

import (
	"archive/tar"
	"bufio"
	"context"
	"errors"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/rs/zerolog/log"
)

type archiveFile struct {
	name     string
	mode     int64
	contents []byte
}

func createArchive(afs []archiveFile) (archivePath string, err error) {
	archive, err := os.CreateTemp("", "archive-*.tar")
	if err != nil {
		return
	}

	buf := bufio.NewWriter(archive)
	tw := tar.NewWriter(buf)

	for _, af := range afs {
		err = tw.WriteHeader(&tar.Header{
			Name: af.name,
			Mode: af.mode,
			Size: int64(len(af.contents)),
		})
		if err != nil {
			return
		}

		_, err = tw.Write(af.contents)
		if err != nil {
			return
		}
	}

	err = buf.Flush()
	if err != nil {
		return
	}

	err = tw.Close()
	if err != nil {
		return
	}

	return archive.Name(), archive.Close()
}

func (d *DockerRuntime) copyArchive(ctx context.Context, archivePath, containerID, destination string) (err error) {
	archive, err := os.Open(archivePath)
	if err != nil {
		return
	}

	defer func() {
		for _, e := range []error{
			archive.Close(),
			os.Remove(archive.Name()),
		} {
			err = errors.Join(err, e)
		}
	}()

	r := bufio.NewReader(archive)

	log.Debug().Msgf("Copying %q to container ID %q, destination %q",
		archive.Name(),
		containerID,
		destination,
	)

	return d.client.CopyToContainer(ctx, containerID, destination, r, types.CopyToContainerOptions{})
}
