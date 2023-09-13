package engine

import (
	"crypto/subtle"
	"fmt"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/internal/coordinator"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/internal/wildcard"
	"github.com/runabol/tork/middleware/job"
	"golang.org/x/time/rate"
)

func (e *Engine) initCoordinator() error {
	queues := conf.IntMap("coordinator.queues")

	cfg := coordinator.Config{
		Broker:    e.broker,
		DataStore: e.ds,
		Queues:    queues,
		Address:   conf.String("coordinator.address"),
		Middleware: coordinator.Middleware{
			Web:  e.cfg.Middleware.Web,
			Task: e.cfg.Middleware.Task,
			Job:  e.cfg.Middleware.Job,
			Node: e.cfg.Middleware.Node,
			Echo: echoMiddleware(),
		},
		Endpoints: e.cfg.Endpoints,
		Enabled:   conf.BoolMap("coordinator.api.endpoints"),
	}

	// redact
	redactJobEnabled := conf.BoolDefault("middleware.job.redact.enabled", false)
	if redactJobEnabled {
		cfg.Middleware.Job = append(cfg.Middleware.Job, job.Redact)
	}

	c, err := coordinator.NewCoordinator(cfg)
	if err != nil {
		return errors.Wrap(err, "error creating the coordinator")
	}

	if err := c.Start(); err != nil {
		return err
	}

	e.coordinator = c

	return nil
}

func echoMiddleware() []echo.MiddlewareFunc {
	mw := make([]echo.MiddlewareFunc, 0)
	// cors
	corsEnabled := conf.Bool("middleware.web.cors.enabled")
	if corsEnabled {
		mw = append(mw, cors())
	}
	// basic auth
	basicAuthEnabled := conf.Bool("middleware.web.basicauth.enabled")
	if basicAuthEnabled {
		username := conf.StringDefault("middleware.web.basicauth.username", "tork")
		password := conf.String("middleware.web.basicauth.password")
		mw = append(mw, basicAuth(username, password))
	}

	// rate limit
	rateLimitEnabled := conf.Bool("middleware.web.ratelimit.enabled")
	if rateLimitEnabled {
		rps := conf.IntDefault("middleware.web.ratelimit.rps", 20)
		mw = append(mw, rateLimit(rps))
	}

	loggerEnabled := conf.BoolDefault("middleware.web.logger.enabled", true)
	if loggerEnabled {
		mw = append(mw, logger())
	}

	return mw
}

func rateLimit(rps int) echo.MiddlewareFunc {
	return middleware.RateLimiter(middleware.NewRateLimiterMemoryStore(rate.Limit(rps)))
}

func basicAuth(username, password string) echo.MiddlewareFunc {
	if password == "" {
		password = uuid.NewUUID()
		log.Debug().Msgf("Basic Auth Password: %s", password)
	}
	return middleware.BasicAuth(func(user, pass string, ctx echo.Context) (bool, error) {
		if subtle.ConstantTimeCompare([]byte(user), []byte(username)) == 1 &&
			subtle.ConstantTimeCompare([]byte(pass), []byte(password)) == 1 {
			return true, nil
		}
		return false, nil
	})
}

func logger() echo.MiddlewareFunc {
	levelStr := conf.StringDefault("middleware.web.logger.level", "DEBUG")
	level, err := zerolog.ParseLevel(strings.ToLower(levelStr))
	if err != nil {
		panic(err)
	}
	skip := conf.StringsDefault("middleware.web.logger.skip", []string{"GET /health"})
	return middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		LogURI:      true,
		LogStatus:   true,
		LogRemoteIP: true,
		LogMethod:   true,
		Skipper: func(c echo.Context) bool {
			if len(skip) == 0 {
				return false
			}
			for _, pattern := range skip {
				if wildcard.Match(pattern, fmt.Sprintf("%s %s", c.Request().Method, c.Request().URL.Path)) {
					return true
				}
			}
			return false
		},
		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
			log.WithLevel(level).
				Str("URI", v.URI).
				Str("method", v.Method).
				Str("remote-ip", v.RemoteIP).
				Int("status", v.Status).
				Msg("Request")
			return nil
		},
	})
}

func cors() echo.MiddlewareFunc {
	type CORSConfig struct {
		AllowOrigins     []string `koanf:"origins"`
		AllowMethods     []string `koanf:"methods"`
		AllowHeaders     []string `koanf:"headers"`
		AllowCredentials bool     `koanf:"credentials"`
		ExposeHeaders    []string `koanf:"headers"`
	}

	cf := CORSConfig{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"*"},
		AllowHeaders:     []string{"*"},
		AllowCredentials: false,
		ExposeHeaders:    []string{"*"},
	}

	if err := conf.Unmarshal("middleware.web.cors", &cf); err != nil {
		panic(errors.Wrapf(err, "error parsing CORS middleware config"))
	}

	log.Debug().Msg("CORS middleware enabled")

	return middleware.CORSWithConfig(
		middleware.CORSConfig{
			AllowOrigins:     cf.AllowOrigins,
			AllowMethods:     cf.AllowMethods,
			AllowHeaders:     cf.AllowHeaders,
			AllowCredentials: cf.AllowCredentials,
			ExposeHeaders:    cf.AllowHeaders,
		},
	)
}
