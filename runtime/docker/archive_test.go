package docker

import (
	"archive/tar"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateArchive(t *testing.T) {
	ar, err := NewTempArchive()
	assert.NoError(t, err)
	assert.NotNil(t, ar)

	err = ar.WriteFile("some_file.txt", 0444, []byte("hello world"))
	assert.NoError(t, err)

	r := tar.NewReader(ar)

	h, err := r.Next()

	assert.NoError(t, err)
	assert.Equal(t, "some_file.txt", h.Name)

	b, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(b))

	assert.NoError(t, ar.Remove())
}
