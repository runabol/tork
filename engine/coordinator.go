package engine

import (
	"crypto/subtle"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/internal/coordinator"
	"github.com/runabol/tork/internal/uuid"
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
		mw = append(mw, basicAuth())
	}

	// rate limit
	rateLimitEnabled := conf.Bool("middleware.web.ratelimit.enabled")
	if rateLimitEnabled {
		mw = append(mw, rateLimit())
	}

	return mw
}

func rateLimit() echo.MiddlewareFunc {
	rps := conf.IntDefault("middleware.web.ratelimit.rps", 20)
	return middleware.RateLimiter(middleware.NewRateLimiterMemoryStore(rate.Limit(rps)))
}

func basicAuth() echo.MiddlewareFunc {
	username := conf.StringDefault("middleware.web.basicauth.username", "tork")
	password := conf.String("middleware.web.basicauth.password")
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

func cors() echo.MiddlewareFunc {
	log.Debug().Msg("CORS middleware enabled")
	return middleware.CORSWithConfig(
		middleware.CORSConfig{
			AllowOrigins: conf.StringsDefault(
				"middleware.web.cors.allow_origins",
				[]string{"*"},
			),
			AllowMethods: conf.StringsDefault(
				"middleware.web.cors.allow_methods",
				[]string{"*"},
			),
			AllowHeaders: conf.StringsDefault(
				"middleware.web.cors.allow_headers",
				[]string{"*"},
			),
			AllowCredentials: conf.Bool(
				"middleware.web.cors.allow_credentials",
			),
			ExposeHeaders: conf.StringsDefault(
				"middleware.web.cors.expose_headers",
				[]string{"*"},
			),
		},
	)
}
