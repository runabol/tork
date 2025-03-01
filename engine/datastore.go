package engine

import (
	"context"

	"github.com/pkg/errors"
	"github.com/runabol/tork"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/datastore/postgres"
)

type datastoreProxy struct {
	ds datastore.Datastore
}

func (ds *datastoreProxy) CreateTask(ctx context.Context, t *tork.Task) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.CreateTask(ctx, t)
}

func (ds *datastoreProxy) UpdateTask(ctx context.Context, id string, modify func(u *tork.Task) error) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.UpdateTask(ctx, id, modify)
}

func (ds *datastoreProxy) GetTaskByID(ctx context.Context, id string) (*tork.Task, error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetTaskByID(ctx, id)
}

func (ds *datastoreProxy) GetActiveTasks(ctx context.Context, jobID string) ([]*tork.Task, error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetActiveTasks(ctx, jobID)
}

func (ds *datastoreProxy) GetNextTask(ctx context.Context, parentTaskID string) (*tork.Task, error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetNextTask(ctx, parentTaskID)
}

func (ds *datastoreProxy) CreateTaskLogPart(ctx context.Context, p *tork.TaskLogPart) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.CreateTaskLogPart(ctx, p)
}

func (ds *datastoreProxy) GetTaskLogParts(ctx context.Context, taskID, q string, page, size int) (*datastore.Page[*tork.TaskLogPart], error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetTaskLogParts(ctx, taskID, q, page, size)
}

func (ds *datastoreProxy) CreateNode(ctx context.Context, n *tork.Node) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.CreateNode(ctx, n)
}

func (ds *datastoreProxy) UpdateNode(ctx context.Context, id string, modify func(u *tork.Node) error) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.UpdateNode(ctx, id, modify)
}

func (ds *datastoreProxy) GetNodeByID(ctx context.Context, id string) (*tork.Node, error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetNodeByID(ctx, id)
}

func (ds *datastoreProxy) GetActiveNodes(ctx context.Context) ([]*tork.Node, error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetActiveNodes(ctx)
}

func (ds *datastoreProxy) CreateJob(ctx context.Context, j *tork.Job) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.CreateJob(ctx, j)
}

func (ds *datastoreProxy) UpdateJob(ctx context.Context, id string, modify func(u *tork.Job) error) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.UpdateJob(ctx, id, modify)
}

func (ds *datastoreProxy) GetJobByID(ctx context.Context, id string) (*tork.Job, error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetJobByID(ctx, id)
}

func (ds *datastoreProxy) GetJobLogParts(ctx context.Context, jobID, q string, page, size int) (*datastore.Page[*tork.TaskLogPart], error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetJobLogParts(ctx, jobID, q, page, size)
}

func (ds *datastoreProxy) GetJobs(ctx context.Context, currentUser, q string, page, size int) (*datastore.Page[*tork.JobSummary], error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetJobs(ctx, currentUser, q, page, size)
}

func (ds *datastoreProxy) CreateScheduledJob(ctx context.Context, s *tork.ScheduledJob) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.CreateScheduledJob(ctx, s)
}

func (ds *datastoreProxy) GetActiveScheduledJobs(ctx context.Context) ([]*tork.ScheduledJob, error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetActiveScheduledJobs(ctx)
}

func (ds *datastoreProxy) GetScheduledJobs(ctx context.Context, currentUser string, page, size int) (*datastore.Page[*tork.ScheduledJobSummary], error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetScheduledJobs(ctx, currentUser, page, size)
}

func (ds *datastoreProxy) GetScheduledJobByID(ctx context.Context, id string) (*tork.ScheduledJob, error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetScheduledJobByID(ctx, id)
}

func (ds *datastoreProxy) UpdateScheduledJob(ctx context.Context, id string, modify func(u *tork.ScheduledJob) error) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.UpdateScheduledJob(ctx, id, modify)
}

func (ds *datastoreProxy) DeleteScheduledJob(ctx context.Context, id string) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.DeleteScheduledJob(ctx, id)
}

func (ds *datastoreProxy) CreateUser(ctx context.Context, u *tork.User) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.CreateUser(ctx, u)
}

func (ds *datastoreProxy) GetUser(ctx context.Context, username string) (*tork.User, error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetUser(ctx, username)
}

func (ds *datastoreProxy) CreateRole(ctx context.Context, r *tork.Role) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.CreateRole(ctx, r)
}

func (ds *datastoreProxy) GetRole(ctx context.Context, id string) (*tork.Role, error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetRole(ctx, id)
}

func (ds *datastoreProxy) GetRoles(ctx context.Context) ([]*tork.Role, error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetRoles(ctx)
}

func (ds *datastoreProxy) GetUserRoles(ctx context.Context, userID string) ([]*tork.Role, error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetUserRoles(ctx, userID)
}

func (ds *datastoreProxy) AssignRole(ctx context.Context, userID, roleID string) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.AssignRole(ctx, userID, roleID)
}

func (ds *datastoreProxy) UnassignRole(ctx context.Context, userID, roleID string) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.UnassignRole(ctx, userID, roleID)
}

func (ds *datastoreProxy) GetMetrics(ctx context.Context) (*tork.Metrics, error) {
	if err := ds.checkInit(); err != nil {
		return nil, err
	}
	return ds.ds.GetMetrics(ctx)
}

func (ds *datastoreProxy) HealthCheck(ctx context.Context) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.HealthCheck(ctx)
}

func (ds *datastoreProxy) WithTx(ctx context.Context, f func(tx datastore.Datastore) error) error {
	if err := ds.checkInit(); err != nil {
		return err
	}
	return ds.ds.WithTx(ctx, f)
}

func (ds *datastoreProxy) checkInit() error {
	if ds.ds == nil {
		return errors.New("Datastore not initialized. You must call engine.Start() first")
	}
	return nil
}

func (e *Engine) initDatastore() error {
	dstype := conf.StringDefault("datastore.type", datastore.DATASTORE_POSTGRES)
	ds, err := e.createDatastore(dstype)
	if err != nil {
		return err
	}
	e.datastoreRef.ds = ds
	return nil
}

func (e *Engine) createDatastore(dstype string) (datastore.Datastore, error) {
	dsp, ok := e.dsProviders[dstype]
	if ok {
		return dsp()
	}
	switch dstype {
	case datastore.DATASTORE_POSTGRES:
		dsn := conf.StringDefault(
			"datastore.postgres.dsn",
			"host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable",
		)
		return postgres.NewPostgresDataStore(dsn,
			postgres.WithLogsRetentionDuration(conf.DurationDefault("datastore.retention.logs.duration", postgres.DefaultLogsRetentionDuration)),
			postgres.WithJobsRetentionDuration(conf.DurationDefault("datastore.retention.jobs.duration", postgres.DefaultJobsRetentionDuration)),
		)
	default:
		return nil, errors.Errorf("unknown datastore type: %s", dstype)
	}
}
