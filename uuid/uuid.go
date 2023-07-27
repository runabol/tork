package uuid

import (
	"strings"

	guuid "github.com/google/uuid"
)

func NewUUID() string {
	return strings.ReplaceAll(guuid.NewString(), "-", "")
}
