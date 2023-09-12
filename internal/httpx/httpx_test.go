package httpx_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/runabol/tork/internal/httpx"
	"github.com/stretchr/testify/assert"
)

func TestStartAsync(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, err := io.WriteString(w, "OK")
		assert.NoError(t, err)
	})
	s := &http.Server{
		Addr:    "localhost:7777",
		Handler: mux,
	}
	err := httpx.StartAsync(s)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, s.Shutdown(context.Background()))
	}()

	req, err := http.NewRequest("GET", "/", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	s.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)
	assert.NoError(t, err)
	assert.Equal(t, "OK", string(body))
}
