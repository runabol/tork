package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/pkg/errors"
	"github.com/runabol/tork/pkg/conf"
	ucli "github.com/urfave/cli/v2"
)

func (c *CLI) healthCmd() *ucli.Command {
	return &ucli.Command{
		Name:   "health",
		Usage:  "Perform a health check",
		Action: health,
	}
}

func health(_ *ucli.Context) error {
	chk, err := http.Get(fmt.Sprintf("%s/health", conf.StringDefault("endpoint", "http://localhost:8000")))
	if err != nil {
		return err
	}
	if chk.StatusCode != http.StatusOK {
		return errors.Errorf("Health check failed. Status Code: %d", chk.StatusCode)
	}
	body, err := io.ReadAll(chk.Body)
	if err != nil {
		return errors.Wrapf(err, "error reading body")
	}

	type resp struct {
		Status string `json:"status"`
	}
	r := resp{}

	if err := json.Unmarshal(body, &r); err != nil {
		return errors.Wrapf(err, "error unmarshalling body")
	}

	fmt.Printf("Status: %s\n", r.Status)

	return nil
}
