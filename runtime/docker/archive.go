package docker

import (
	"archive/tar"
	"bufio"
	"os"

	"github.com/pkg/errors"
)

type archive struct {
	f      *os.File
	reader *bufio.Reader
	writer *tar.Writer
}

func NewTempArchive() (*archive, error) {
	f, err := os.CreateTemp("", "archive-*.tar")
	if err != nil {
		return nil, errors.Wrapf(err, "error creating temp archive file")
	}
	a := &archive{
		f:      f,
		writer: tar.NewWriter(f),
	}
	return a, nil
}

func (a *archive) Read(p []byte) (int, error) {
	if a.reader == nil {
		if err := a.f.Close(); err != nil {
			return 0, err
		}
		f, err := os.Open(a.f.Name())
		if err != nil {
			return 0, err
		}
		a.f = f
		a.reader = bufio.NewReader(f)
	}
	n, err := a.reader.Read(p)
	if err != nil {
		if err := a.f.Close(); err != nil {
			return 0, err
		}
	}
	return n, err
}

func (a *archive) Name() string {
	return a.f.Name()
}

func (a *archive) Remove() error {
	return os.Remove(a.f.Name())
}

func (a *archive) WriteFile(name string, mode int64, contents []byte) error {
	hdr := &tar.Header{
		Name: name,
		Mode: mode,
		Size: int64(len(contents)),
	}
	if err := a.writer.WriteHeader(hdr); err != nil {
		return err
	}
	if _, err := a.writer.Write(contents); err != nil {
		return err
	}
	return nil
}
