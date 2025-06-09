package fns

import (
	"bytes"
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

func TestFprintf(t *testing.T) {
	var buf bytes.Buffer
	format := "Hello, %s!"
	name := "World"
	expected := "Hello, World!"

	Fprintf(&buf, format, name)

	if buf.String() != expected {
		t.Errorf("expected: %q, got: %q", expected, buf.String())
	}
}

func TestFprintf_EmptyWriter(t *testing.T) {
	var buf bytes.Buffer
	format := ""
	expected := ""

	Fprintf(&buf, format)

	if buf.String() != expected {
		t.Errorf("expected: %q, got: %q", expected, buf.String())
	}
}

func TestFprintf_ErrorWriter(t *testing.T) {
	errorWriter := &mockErrorWriter{}
	format := "Hello, %s!"
	name := "World"

	Fprintf(errorWriter, format, name)

	if !errorWriter.writeCalled {
		t.Errorf("expected write to be called, but it was not")
	}
}

type mockErrorWriter struct {
	writeCalled bool
}

func (m *mockErrorWriter) Write(p []byte) (n int, err error) {
	m.writeCalled = true
	return 0, errors.New("mock write error")
}
