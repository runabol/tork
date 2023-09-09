package engine

// OnStartedHandler a bootstrap hook that is
// called after Tork has finished starting up.
// If a non-nil error is returned it will
// terminate the bootstrap process.
type OnStartedHandler func() error
