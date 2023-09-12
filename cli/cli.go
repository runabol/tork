package cli

import (
	"os"

	"github.com/runabol/tork/engine"
	"github.com/runabol/tork/internal/logging"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/middleware/node"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/middleware/web"
	ucli "github.com/urfave/cli/v2"
)

type CLI struct {
	app *ucli.App
}

func New() *CLI {
	app := &ucli.App{
		Name:  "tork",
		Usage: "a distributed workflow engine",
	}
	c := &CLI{
		app: app,
	}
	app.Before = c.before
	app.Commands = c.commands()
	return c
}

func (c *CLI) RegisterWebMiddleware(mw web.MiddlewareFunc) {
	engine.RegisterWebMiddleware(mw)
}

func (c *CLI) RegisterTaskMiddleware(mw task.MiddlewareFunc) {
	engine.RegisterTaskMiddleware(mw)
}

func (c *CLI) RegisterJobMiddleware(mw job.MiddlewareFunc) {
	engine.RegisterJobMiddleware(mw)
}

func (c *CLI) RegisterNodeMiddleware(mw node.MiddlewareFunc) {
	engine.RegisterNodeMiddleware(mw)
}

func (c *CLI) RegisterEndpoint(method, path string, handler web.HandlerFunc) {
	engine.RegisterEndpoint(method, path, handler)
}

func (c *CLI) Run() error {
	return c.app.Run(os.Args)
}

func (c *CLI) before(ctx *ucli.Context) error {
	displayBanner()

	if err := logging.SetupLogging(); err != nil {
		return err
	}
	return nil
}

func (c *CLI) commands() []*ucli.Command {
	return []*ucli.Command{
		c.runCmd(),
		c.migrationCmd(),
		c.healthCmd(),
	}
}
