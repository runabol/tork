package uuid

import (
	"encoding/base64"
	"strings"

	guuid "github.com/google/uuid"
)

// NewUUID creates a new random UUID and returns it as a string or panics.
func NewUUID() string {
	return strings.ReplaceAll(guuid.NewString(), "-", "")
}

// NewUUID creates a new random UUID and encodes it in base64 or panics.
func NewUUIDBase64() string {
	u := guuid.New()
	return base64.RawURLEncoding.EncodeToString(u[:])
}
