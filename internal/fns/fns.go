package fns

import "io"

// CloseIgnore closes c, ignoring any error.
// Its main use is to satisfy linters.
func CloseIgnore(c io.Closer) {
	_ = c.Close()
}
