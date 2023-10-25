package docker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	ref, err := parseRef("ubuntu:mantic")
	assert.NoError(t, err)
	assert.Equal(t, "", ref.domain)
	assert.Equal(t, "ubuntu", ref.path)
	assert.Equal(t, "mantic", ref.tag)

	ref, err = parseRef("localhost:9090/ubuntu:mantic")
	assert.NoError(t, err)
	assert.Equal(t, "localhost:9090", ref.domain)
	assert.Equal(t, "ubuntu", ref.path)
	assert.Equal(t, "mantic", ref.tag)

	ref, err = parseRef("localhost:9090/ubuntu:mantic-2.7")
	assert.NoError(t, err)
	assert.Equal(t, "localhost:9090", ref.domain)
	assert.Equal(t, "ubuntu", ref.path)
	assert.Equal(t, "mantic-2.7", ref.tag)

	ref, err = parseRef("my-registry/ubuntu:mantic-2.7")
	assert.NoError(t, err)
	assert.Equal(t, "my-registry", ref.domain)
	assert.Equal(t, "ubuntu", ref.path)
	assert.Equal(t, "mantic-2.7", ref.tag)

	ref, err = parseRef("my-registry/ubuntu")
	assert.NoError(t, err)
	assert.Equal(t, "my-registry", ref.domain)
	assert.Equal(t, "ubuntu", ref.path)
	assert.Equal(t, "", ref.tag)

	ref, err = parseRef("ubuntu")
	assert.NoError(t, err)
	assert.Equal(t, "", ref.domain)
	assert.Equal(t, "ubuntu", ref.path)
	assert.Equal(t, "", ref.tag)

	ref, err = parseRef("ubuntu:latest")
	assert.NoError(t, err)
	assert.Equal(t, "", ref.domain)
	assert.Equal(t, "ubuntu", ref.path)
	assert.Equal(t, "latest", ref.tag)
}
