package encrypt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncrypt(t *testing.T) {
	ciphertext, err := Encrypt("hello", "secret")
	assert.NoError(t, err)
	assert.NotEqual(t, "hello", ciphertext)

	decrypted, err := Decrypt(ciphertext, "secret")
	assert.NoError(t, err)
	assert.Equal(t, "hello", decrypted)

	decrypted, err = Decrypt(ciphertext, "bad_secret")
	assert.Error(t, err)
	assert.Equal(t, "", decrypted)
}
