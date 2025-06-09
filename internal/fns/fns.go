package fns

import (
	"fmt"
	"io"
)

// CloseIgnore closes c, ignoring any error.
// Its main use is to satisfy linters.
func CloseIgnore(c io.Closer) {
	_ = c.Close()
}

func Fprintf(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintf(w, format, a...)
}
