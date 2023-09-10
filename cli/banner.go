package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/pkg/conf"
)

func displayBanner() {
	mode := conf.StringDefault("cli.banner.mode", "console")
	if mode == "off" {
		return
	}
	banner := color.WhiteString(fmt.Sprintf(`
 _______  _______  ______    ___   _ 
|       ||       ||    _ |  |   | | |
|_     _||   _   ||   | ||  |   |_| |
  |   |  |  | |  ||   |_||_ |      _|
  |   |  |  |_|  ||    __  ||     |_ 
  |   |  |       ||   |  | ||    _  |
  |___|  |_______||___|  |_||___| |_|

 %s (%s)
`, tork.Version, tork.GitCommit))

	if mode == "console" {
		fmt.Println(banner)
	} else {
		log.Info().Msg(banner)
	}
}
