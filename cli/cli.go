package cli

import (
	"fmt"
	"os"

	"github.com/runabol/tork/internal/logging"
	"github.com/runabol/tork/pkg/engine"
	"github.com/runabol/tork/pkg/middleware/job"
	"github.com/runabol/tork/pkg/middleware/node"
	"github.com/runabol/tork/pkg/middleware/task"
	"github.com/runabol/tork/pkg/middleware/web"
	ucli "github.com/urfave/cli/v2"
)

type CLI struct {
	app         *ucli.App
	configurers []func(eng *engine.Engine) error
	webmw       []web.MiddlewareFunc
	taskmw      []task.MiddlewareFunc
	jobmw       []job.MiddlewareFunc
	nodemw      []node.MiddlewareFunc
	endpoints   map[string]web.HandlerFunc
}

func New() *CLI {
	app := &ucli.App{
		Name:  "tork",
		Usage: "a distributed workflow engine",
	}
	c := &CLI{
		app:       app,
		endpoints: make(map[string]web.HandlerFunc),
	}
	app.Before = c.before
	app.Commands = c.commands()
	return c
}

func (c *CLI) RegisterWebMiddleware(mw web.MiddlewareFunc) {
	c.webmw = append(c.webmw, mw)
}

func (c *CLI) RegisterTaskMiddleware(mw task.MiddlewareFunc) {
	c.taskmw = append(c.taskmw, mw)
}

func (c *CLI) RegisterJobMiddleware(mw job.MiddlewareFunc) {
	c.jobmw = append(c.jobmw, mw)
}

func (c *CLI) RegisterNodeMiddleware(mw node.MiddlewareFunc) {
	c.nodemw = append(c.nodemw, mw)
}

func (c *CLI) RegisterEndpoint(method, path string, handler web.HandlerFunc) {
	c.endpoints[fmt.Sprintf("%s %s", method, path)] = handler
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
