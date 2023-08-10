package uuid

import (
	"strings"

	guuid "github.com/google/uuid"
)

// NewUUID creates a new random UUID and returns it as a string or panics.
func NewUUID() string {
	return strings.ReplaceAll(guuid.NewString(), "-", "")
}
