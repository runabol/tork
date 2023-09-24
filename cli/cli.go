package cli

import (
	"os"

	"github.com/runabol/tork/internal/logging"
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
