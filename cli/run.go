package cli

import (
	"fmt"
	"os"

	"github.com/runabol/tork/pkg/engine"
	ucli "github.com/urfave/cli/v2"
)

func (c *CLI) runCmd() *ucli.Command {
	return &ucli.Command{
		Name:      "run",
		Usage:     "Run Tork",
		UsageText: "tork run mode (standalone|coordinator|worker)",
		Action:    c.run,
	}
}
func (c *CLI) run(ctx *ucli.Context) error {
	mode := ctx.Args().First()
	if mode == "" {
		if err := ucli.ShowSubcommandHelp(ctx); err != nil {
			return err
		}
		fmt.Println("missing required argument: mode")
		os.Exit(1)

	}
	eng := engine.New(engine.Config{
		Middleware: engine.Middleware{
			Web:  c.webmw,
			Task: c.taskmw,
			Job:  c.jobmw,
			Node: c.nodemw,
		},
		Endpoints: c.endpoints,
		Mode:      engine.Mode(ctx.Args().First()),
	})
	for _, co := range c.configurers {
		if err := co(eng); err != nil {
			return err
		}
	}
	return eng.Start()
}
