package cli

import "github.com/runabol/tork/bootstrap"

// OnRunHandler is a hook interface allowing the
// calling code to override the default CLI handling
// of the "run" command.
type OnRunHandler func(mode bootstrap.Mode) error
