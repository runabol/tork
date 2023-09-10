package conf

import (
	"fmt"
	"os"
	"strings"

	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/env"
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

func LoadConfig() error {
	var paths []string
	userConfig := os.Getenv("TORK_CONFIG")
	if userConfig != "" {
		paths = []string{userConfig}
	} else {
		paths = defaultConfigPaths
	}
	// load configs from file paths
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
	// load configs from env vars
	if err := konf.Load(env.Provider("TORK_", ".", func(s string) string {
		return strings.Replace(strings.ToLower(
			strings.TrimPrefix(s, "TORK_")), "_", ".", -1)
	}), nil); err != nil {
		return errors.Wrapf(err, "error loading config from env")
	}
	errMsg := fmt.Sprintf("could not find config file in any of the following paths: %s", strings.Join(paths, ","))
	if userConfig != "" {
		return errors.Errorf(errMsg)
	} else {
		logger.Warn().Msg(errMsg)
	}
	return nil
}

func IntMap(key string) map[string]int {
	return konf.IntMap(key)
}

func BoolMap(key string) map[string]bool {
	return konf.BoolMap(key)
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

func Bool(key string) bool {
	return konf.Bool(key)
}

func BoolDefault(key string, dv bool) bool {
	v := konf.String(key)
	if v != "" {
		return Bool(key)
	}
	return dv
}
