package signals

import (
	"os"
	"os/signal"
	"syscall"
)

func AwaitTerm() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}
