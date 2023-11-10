package uuid

import (
	"strings"

	guuid "github.com/google/uuid"
	"github.com/lithammer/shortuuid/v4"
)

// NewUUID creates a new random UUID and returns it as a string or panics.
func NewUUID() string {
	return strings.ReplaceAll(guuid.NewString(), "-", "")
}

// NewShortUUID returns a new UUIDv4, encoded with base57
func NewShortUUID() string {
	return shortuuid.New()
}
