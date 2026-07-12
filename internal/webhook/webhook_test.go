package webhook

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func TestCall(t *testing.T) {
	// Test Cases
	tests := []struct {
		name          string
		responseCodes []int // Sequence of response codes to return
		numRequests   int   // Number of requests expected
		expectedError bool  // Should the function return an error?
	}{
		{
			name:          "Successful Response",
			responseCodes: []int{http.StatusOK},
			numRequests:   1,
			expectedError: false,
		},
		{
			name:          "Successful Response",
			responseCodes: []int{http.StatusNoContent},
			numRequests:   1,
			expectedError: false,
		},
		{
			name:          "Retryable Response - 500 Internal Server Error",
			responseCodes: []int{http.StatusInternalServerError, http.StatusInternalServerError, http.StatusOK},
			numRequests:   3,
			expectedError: false,
		},
		{
			name:          "Non-Retryable Response - 400 Bad Request",
			responseCodes: []int{http.StatusBadRequest},
			numRequests:   1,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test server that returns responses in sequence
			requestCount := 0
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if requestCount < len(tt.responseCodes) {
					w.WriteHeader(tt.responseCodes[requestCount])
					requestCount++
				}
			}))
			defer testServer.Close()

			// Prepare the Webhook configuration
			wh := &tork.Webhook{
				URL: testServer.URL,
			}
			body := map[string]string{"key": "value"}

			// Call the function
			err := Call(wh, body)

			// Check retries and errors
			assert.Equal(t, tt.numRequests, requestCount, "Number of requests sent does not match expected")
			if tt.expectedError {
				assert.Error(t, err, "Expected an error but got nil")
			} else {
				assert.NoError(t, err, "Did not expect an error but got one")
			}
		})
	}
}

func TestCallClosesResponseBodyOnRetry(t *testing.T) {
	// The webhook retries on 5xx responses. Previously the response body
	// was only closed via a deferred call that ran after the entire retry
	// loop finished, so every failed attempt leaked its body. This test
	// asserts that each attempt's body is closed as soon as it is consumed.
	closed := 0
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("boom"))
	}))
	defer testServer.Close()

	client := &http.Client{
		Timeout: webhookDefaultTimeout,
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			resp, err := http.DefaultTransport.RoundTrip(r)
			if err != nil {
				return resp, err
			}
			orig := resp.Body
			resp.Body = &trackingReadCloser{ReadCloser: orig, onClose: func() { closed++ }}
			return resp, nil
		}),
	}

	wh := &tork.Webhook{URL: testServer.URL}
	err := callWithClient(wh, map[string]string{"key": "value"}, client)
	assert.Error(t, err)

	// Five attempts means five responses; all of them must be closed.
	assert.Equal(t, webhookDefaultMaxAttempts, closed, "expected every attempt's response body to be closed")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type trackingReadCloser struct {
	io.ReadCloser
	onClose func()
}

func (t *trackingReadCloser) Close() error {
	t.onClose()
	return t.ReadCloser.Close()
}
