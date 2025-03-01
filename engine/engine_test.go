package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/datastore/postgres"
	"github.com/runabol/tork/input"
	"github.com/runabol/tork/internal/hash"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/runtime/docker"
	"github.com/runabol/tork/runtime/shell"

	"github.com/runabol/tork/runtime"
	"github.com/stretchr/testify/assert"
)

func TestStartStandalone(t *testing.T) {
	eng := New(Config{
		Mode: ModeStandalone,
	})

	assert.Equal(t, StateIdle, eng.State())

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)

	eng.RegisterDatastoreProvider(datastore.DATASTORE_POSTGRES, func() (datastore.Datastore, error) {
		return ds, nil
	})

	err = eng.Start()
	assert.NoError(t, err)

	assert.Equal(t, StateRunning, eng.State())
	err = eng.Terminate()
	assert.NoError(t, err)
	assert.Equal(t, StateTerminated, eng.State())
	assert.NoError(t, ds.Close())
}

func TestRunStandalone(t *testing.T) {
	eng := New(Config{
		Mode: ModeStandalone,
	})

	assert.Equal(t, StateIdle, eng.state)

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)

	eng.RegisterDatastoreProvider(datastore.DATASTORE_POSTGRES, func() (datastore.Datastore, error) {
		return ds, nil
	})

	err = eng.Start()
	assert.NoError(t, err)

	assert.Equal(t, StateRunning, eng.State())
	err = eng.Terminate()
	assert.NoError(t, err)
	assert.Equal(t, StateTerminated, eng.State())
	assert.NoError(t, ds.Close())
}

func TestStartCoordinator(t *testing.T) {
	eng := New(Config{Mode: ModeCoordinator})

	assert.Equal(t, StateIdle, eng.state)

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)

	eng.RegisterDatastoreProvider(datastore.DATASTORE_POSTGRES, func() (datastore.Datastore, error) {
		return ds, nil
	})

	err = eng.Start()
	assert.NoError(t, err)
	assert.Equal(t, StateRunning, eng.state)

	err = eng.Terminate()
	assert.NoError(t, err)
	assert.Equal(t, StateTerminated, eng.state)
	assert.NoError(t, ds.Close())
}

func TestStartWorker(t *testing.T) {
	eng := New(Config{Mode: ModeWorker})

	assert.Equal(t, StateIdle, eng.state)

	err := eng.Start()
	assert.NoError(t, err)
	assert.Equal(t, StateRunning, eng.state)

	err = eng.Terminate()
	assert.NoError(t, err)
	assert.Equal(t, StateTerminated, eng.state)
}

func Test_basicAuthWrongPassword(t *testing.T) {
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	password := uuid.NewShortUUID()
	hashedPassword, err := hash.Password(password)
	assert.NoError(t, err)
	u := &tork.User{
		ID:           uuid.NewUUID(),
		Username:     uuid.NewShortUUID(),
		Name:         "Tester",
		PasswordHash: hashedPassword,
	}
	err = ds.CreateUser(context.Background(), u)
	assert.NoError(t, err)
	mw := basicAuth(ds)
	req, err := http.NewRequest("GET", "/health", nil)
	req.SetBasicAuth(u.Username, "wrong")
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	ctx := echo.New().NewContext(req, w)
	h := func(c echo.Context) error {
		return nil
	}
	x := mw(h)
	err = x(ctx)
	assert.Error(t, err)
	assert.NoError(t, ds.Close())
}

func Test_basicCorrectPassword(t *testing.T) {
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	password := uuid.NewShortUUID()
	hashedPassword, err := hash.Password(password)
	assert.NoError(t, err)
	u := &tork.User{
		ID:           uuid.NewUUID(),
		Username:     uuid.NewShortUUID(),
		Name:         "Tester",
		PasswordHash: hashedPassword,
	}
	err = ds.CreateUser(context.Background(), u)
	assert.NoError(t, err)
	mw := basicAuth(ds)
	req, err := http.NewRequest("GET", "/health", nil)
	req.SetBasicAuth(u.Username, password)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	ctx := echo.New().NewContext(req, w)
	h := func(c echo.Context) error {
		return nil
	}
	x := mw(h)
	err = x(ctx)
	assert.NoError(t, err)
	assert.NoError(t, ds.Close())
}

func Test_rateLimit(t *testing.T) {
	mw := rateLimit(20)
	req, err := http.NewRequest("GET", "/health", nil)
	req.SetBasicAuth("tork", "password")
	req.Header.Set("X-Real-Ip", "1.1.1.1")
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	ctx := echo.New().NewContext(req, w)
	h := func(c echo.Context) error {
		return nil
	}
	for i := 0; i < 30; i++ {
		x := mw(h)
		err = x(ctx)
		assert.NoError(t, err)
		if i < 20 {
			assert.Equal(t, http.StatusOK, w.Code)
		} else {
			assert.Equal(t, http.StatusTooManyRequests, w.Code)
		}
	}
}

func Test_rateLimitCustomRPS(t *testing.T) {
	mw := rateLimit(10)
	req, err := http.NewRequest("GET", "/health", nil)
	req.SetBasicAuth("tork", "password")
	req.Header.Set("X-Real-Ip", "1.1.1.1")
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	ctx := echo.New().NewContext(req, w)
	h := func(c echo.Context) error {
		return nil
	}
	for i := 0; i < 30; i++ {
		x := mw(h)
		err = x(ctx)
		assert.NoError(t, err)
		if i < 10 {
			assert.Equal(t, http.StatusOK, w.Code)
		} else {
			assert.Equal(t, http.StatusTooManyRequests, w.Code)
		}
	}
}

func TestSubmitJob(t *testing.T) {
	eng := New(Config{Mode: ModeCoordinator})

	assert.Equal(t, StateIdle, eng.state)

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)

	eng.RegisterDatastoreProvider(datastore.DATASTORE_POSTGRES, func() (datastore.Datastore, error) {
		return ds, nil
	})

	err = eng.Start()
	assert.NoError(t, err)
	assert.Equal(t, StateRunning, eng.state)

	c := make(chan any)

	listener := func(j *tork.Job) {
		close(c)
		assert.Equal(t, tork.JobStateCompleted, j.State)
	}

	ctx := context.Background()

	j, err := eng.SubmitJob(ctx, &input.Job{
		Name: "test job",
		Tasks: []input.Task{
			{
				Name:  "first task",
				Image: "some:image",
			},
		},
	}, listener)
	assert.NoError(t, err)

	j.State = tork.JobStateCompleted

	err = eng.Broker().PublishEvent(context.Background(), broker.TOPIC_JOB_COMPLETED, j)
	assert.NoError(t, err)
	<-c
	assert.NoError(t, ds.Close())
}

func TestSubmitJobPanics(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})
	assert.Panics(t, func() {
		_, _ = eng.SubmitJob(context.Background(), &input.Job{})
	})
}

func TestSubmitInvalidJob(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})

	assert.Equal(t, StateIdle, eng.state)

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)

	eng.RegisterDatastoreProvider(datastore.DATASTORE_POSTGRES, func() (datastore.Datastore, error) {
		return ds, nil
	})

	err = eng.Start()
	assert.NoError(t, err)
	assert.Equal(t, StateRunning, eng.state)

	ctx := context.Background()

	_, err = eng.SubmitJob(ctx, &input.Job{})
	assert.Error(t, err)
	assert.NoError(t, ds.Close())
}

func TestRegisterMounter(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})
	assert.Equal(t, StateIdle, eng.state)

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)

	eng.RegisterDatastoreProvider(datastore.DATASTORE_POSTGRES, func() (datastore.Datastore, error) {
		return ds, nil
	})

	eng.RegisterMounter(runtime.Docker, "bind2", docker.NewBindMounter(docker.BindConfig{}))

	err = eng.Start()
	assert.NoError(t, err)
	assert.Equal(t, StateRunning, eng.state)

	err = eng.Terminate()
	assert.NoError(t, err)
	assert.NoError(t, ds.Close())
}

func TestRegisterRuntime(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})
	assert.Equal(t, StateIdle, eng.state)

	eng.RegisterRuntime(shell.NewShellRuntime(shell.Config{}))
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	eng.RegisterDatastoreProvider(datastore.DATASTORE_POSTGRES, func() (datastore.Datastore, error) {
		return ds, nil
	})

	err = eng.Start()
	assert.NoError(t, err)
	assert.Equal(t, StateRunning, eng.state)

	err = eng.Terminate()
	assert.NoError(t, err)
	assert.NoError(t, ds.Close())
}

func TestOnBrokerInit(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})
	assert.Equal(t, StateIdle, eng.state)

	b := eng.Broker()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)

	eng.RegisterDatastoreProvider(datastore.DATASTORE_POSTGRES, func() (datastore.Datastore, error) {
		return ds, nil
	})

	assert.Error(t, b.HealthCheck(context.Background()))

	err = eng.Start()
	assert.NoError(t, err)
	assert.Equal(t, StateRunning, eng.state)

	assert.NoError(t, b.HealthCheck(context.Background()))

	err = eng.Terminate()
	assert.NoError(t, err)

	assert.NoError(t, ds.Close())
}

func TestOnDatastoreInit(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})
	assert.Equal(t, StateIdle, eng.state)

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)

	eng.RegisterDatastoreProvider(datastore.DATASTORE_POSTGRES, func() (datastore.Datastore, error) {
		return ds, nil
	})

	ds2 := eng.Datastore()
	assert.Error(t, ds2.HealthCheck(context.Background()))

	err = eng.Start()
	assert.NoError(t, err)
	assert.Equal(t, StateRunning, eng.state)
	assert.NoError(t, ds.HealthCheck(context.Background()))

	err = eng.Terminate()
	assert.NoError(t, err)
	assert.NoError(t, ds.Close())
}
