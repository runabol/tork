package conf

import (
	"fmt"
	"os"
	"strings"

	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var konf = koanf.New(".")
var logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

var defaultConfigPaths = []string{
	"config.local.toml",
	"config.toml",
	"~/tork/config.toml",
	"/etc/tork/config.toml",
}

func LoadConfig(paths ...string) error {
	userSpecified := len(paths) > 0
	if len(paths) == 0 {
		paths = defaultConfigPaths
	}
	for _, f := range paths {
		err := konf.Load(file.Provider(f), toml.Parser())
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return errors.Wrapf(err, "error loading config from %s", f)
		}
		logger.Info().Msgf("Config loaded from %s", f)
		return nil
	}
	errMsg := fmt.Sprintf("could not find config file in any of the following paths: %s", strings.Join(paths, ","))
	if userSpecified {
		return errors.Errorf(errMsg)
	} else {
		logger.Warn().Msg(errMsg)
	}
	return nil
}

func IntMap(key string) map[string]int {
	return konf.IntMap(key)
}

func String(key string) string {
	return konf.String(key)
}

func StringDefault(key, dv string) string {
	v := String(key)
	if v != "" {
		return v
	}
	return dv
}
