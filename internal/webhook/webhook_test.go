package webhook

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func TestCall(t *testing.T) {
	// Test Cases
	tests := []struct {
		name            string
		responseCodes   []int // Sequence of response codes to return
		expectedRetries int   // Expected retry attempts
		expectedError   bool  // Should the function return an error?
	}{
		{
			name:            "Successful Response",
			responseCodes:   []int{http.StatusOK},
			expectedRetries: 1,
			expectedError:   false,
		},
		{
			name:            "Retryable Response - 500 Internal Server Error",
			responseCodes:   []int{http.StatusInternalServerError, http.StatusOK},
			expectedRetries: 2,
			expectedError:   false,
		},
		{
			name:            "Non-Retryable Response - 400 Bad Request",
			responseCodes:   []int{http.StatusBadRequest},
			expectedRetries: 1,
			expectedError:   false,
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
			assert.Equal(t, tt.expectedRetries, requestCount, "Number of retries should match expected")
			if tt.expectedError {
				assert.Error(t, err, "Expected an error but got nil")
			} else {
				assert.NoError(t, err, "Did not expect an error but got one")
			}
		})
	}
}
