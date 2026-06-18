package fns

import (
	"errors"
	"testing"
)

type mockCloser struct {
	shouldError bool
	closed      bool
}

func (m *mockCloser) Close() error {
	m.closed = true
	if m.shouldError {
		return errors.New("mock error")
	}
	return nil
}

func TestCloseIgnore(t *testing.T) {
	tests := []struct {
		name           string
		closer         *mockCloser
		expectedClosed bool
	}{
		{
			name:           "Close without error",
			closer:         &mockCloser{shouldError: false},
			expectedClosed: true,
		},
		{
			name:           "Close with error",
			closer:         &mockCloser{shouldError: true},
			expectedClosed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			CloseIgnore(tt.closer)
			if tt.closer.closed != tt.expectedClosed {
				t.Errorf("expected closed: %v, got: %v", tt.expectedClosed, tt.closer.closed)
			}
		})
	}
}
