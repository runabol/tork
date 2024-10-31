package inmemory

import (
	"context"
	"strings"
	"sync"

	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/runabol/tork"

	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/cache"
	"github.com/runabol/tork/internal/slices"
	"github.com/runabol/tork/internal/uuid"
)

const (
	defaultNodeExpiration  = time.Minute * 10
	defaultCleanupInterval = time.Minute * 10
	defaultJobExpiration   = time.Hour
)

var guestUser = &tork.User{
	ID:       "guest",
	Name:     "Guest",
	Username: "Guest",
	Disabled: true,
}

type InMemoryDatastore struct {
	tasks           *cache.Cache[*tork.Task]
	nodes           *cache.Cache[*tork.Node]
	jobs            *cache.Cache[*tork.Job]
	usersByID       *cache.Cache[*tork.User]
	usersByUsername *cache.Cache[*tork.User]
	roles           *cache.Cache[*tork.Role]
	userRoles       *cache.Cache[[]*tork.UserRole]
	logs            *cache.Cache[[]*tork.TaskLogPart]
	logsMu          sync.RWMutex
	nodeExpiration  *time.Duration
	jobExpiration   *time.Duration
	cleanupInterval *time.Duration
}

type Option = func(ds *InMemoryDatastore)

func WithNodeExpiration(exp time.Duration) Option {
	return func(ds *InMemoryDatastore) {
		ds.nodeExpiration = &exp
	}
}

func WithJobExpiration(exp time.Duration) Option {
	return func(ds *InMemoryDatastore) {
		ds.jobExpiration = &exp
	}
}
func WithCleanupInterval(ci time.Duration) Option {
	return func(ds *InMemoryDatastore) {
		ds.cleanupInterval = &ci
	}
}

func NewInMemoryDatastore(opts ...Option) *InMemoryDatastore {
	ds := &InMemoryDatastore{}
	for _, opt := range opts {
		opt(ds)
	}
	ci := defaultCleanupInterval
	if ds.cleanupInterval != nil {
		ci = *ds.cleanupInterval
	}
	ds.tasks = cache.New[*tork.Task](cache.NoExpiration, ci)
	nodeExp := defaultNodeExpiration
	if ds.nodeExpiration != nil {
		nodeExp = *ds.nodeExpiration
	}
	ds.nodes = cache.New[*tork.Node](nodeExp, ci)
	ds.jobs = cache.New[*tork.Job](cache.NoExpiration, ci)
	ds.logs = cache.New[[]*tork.TaskLogPart](cache.NoExpiration, ci)
	ds.usersByID = cache.New[*tork.User](cache.NoExpiration, ci)
	ds.usersByUsername = cache.New[*tork.User](cache.NoExpiration, ci)
	ds.roles = cache.New[*tork.Role](cache.NoExpiration, ci)
	ds.userRoles = cache.New[[]*tork.UserRole](cache.NoExpiration, ci)
	ds.jobs.OnEvicted(ds.onJobEviction)
	return ds
}

func (ds *InMemoryDatastore) CreateTask(ctx context.Context, t *tork.Task) error {
	if t.ID == "" {
		return errors.New("must provide ID")
	}
	ds.tasks.Set(t.ID, t.Clone())
	return nil
}

func (ds *InMemoryDatastore) GetTaskByID(ctx context.Context, id string) (*tork.Task, error) {
	t, ok := ds.tasks.Get(id)
	if !ok {
		return nil, datastore.ErrTaskNotFound
	}
	return t.Clone(), nil
}

func (ds *InMemoryDatastore) UpdateTask(ctx context.Context, id string, modify func(u *tork.Task) error) error {
	_, ok := ds.tasks.Get(id)
	if !ok {
		return datastore.ErrTaskNotFound
	}
	return ds.tasks.Modify(id, func(t *tork.Task) (*tork.Task, error) {
		update := t.Clone()
		if err := modify(update); err != nil {
			return nil, errors.Wrapf(err, "error modifying task %s", id)
		}
		return update, nil
	})
}

func (ds *InMemoryDatastore) CreateNode(ctx context.Context, n *tork.Node) error {
	_, ok := ds.nodes.Get(n.ID)
	if ok {
		return errors.Errorf("node %s already exists", n.ID)
	}
	ds.nodes.Set(n.ID, n.Clone())
	return nil
}

func (ds *InMemoryDatastore) UpdateNode(ctx context.Context, id string, modify func(u *tork.Node) error) error {
	_, ok := ds.nodes.Get(id)
	if !ok {
		return datastore.ErrNodeNotFound
	}
	return ds.nodes.Modify(id, func(n *tork.Node) (*tork.Node, error) {
		update := n.Clone()
		if err := modify(update); err != nil {
			return nil, errors.Wrapf(err, "error modifying node %s", id)
		}
		return update, nil
	})
}

func (ds *InMemoryDatastore) GetNodeByID(ctx context.Context, id string) (*tork.Node, error) {
	n, ok := ds.nodes.Get(id)
	if !ok {
		return nil, datastore.ErrNodeNotFound
	}
	return n.Clone(), nil
}

func (ds *InMemoryDatastore) GetActiveNodes(ctx context.Context) ([]*tork.Node, error) {
	nodes := make([]*tork.Node, 0)
	timeout := time.Now().UTC().Add(-tork.LAST_HEARTBEAT_TIMEOUT)
	ds.nodes.Iterate(func(_ string, n *tork.Node) {
		if n.LastHeartbeatAt.After(timeout) {
			// if we hadn't seen an heartbeat for two or more
			// consecutive periods we consider the node as offline
			if n.LastHeartbeatAt.Before(time.Now().UTC().Add(-tork.HEARTBEAT_RATE * 2)) {
				n.Status = tork.NodeStatusOffline
			}
			nodes = append(nodes, n.Clone())
		}
	})
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].LastHeartbeatAt.After(nodes[j].LastHeartbeatAt)
	})
	return nodes, nil
}

func (ds *InMemoryDatastore) CreateJob(ctx context.Context, j *tork.Job) error {
	if j.CreatedBy == nil {
		j.CreatedBy = guestUser
	}
	ds.jobs.Set(j.ID, j.Clone())
	return nil
}

func (ds *InMemoryDatastore) UpdateJob(ctx context.Context, id string, modify func(u *tork.Job) error) error {
	_, ok := ds.jobs.Get(id)
	if !ok {
		return datastore.ErrJobNotFound
	}

	err := ds.jobs.Modify(id, func(j *tork.Job) (*tork.Job, error) {
		update := j.Clone()
		if err := modify(update); err != nil {
			return nil, errors.Wrapf(err, "error modifying job %s", id)
		}
		return update, nil
	})

	if err != nil {
		return err
	}

	j, ok := ds.jobs.Get(id)
	if !ok {
		return datastore.ErrJobNotFound
	}

	if j.State == tork.JobStateCompleted || j.State == tork.JobStateFailed {
		exp := defaultJobExpiration
		if ds.jobExpiration != nil {
			exp = *ds.jobExpiration
		}
		if err := ds.jobs.SetExpiration(j.ID, exp); err != nil {
			return errors.Wrap(err, "error modifying job expiration")
		}
	}

	return nil
}

func (ds *InMemoryDatastore) getExecution(id string) []*tork.Task {
	execution := make([]*tork.Task, 0)
	ds.tasks.Iterate(func(_ string, t *tork.Task) {
		if t.JobID == id {
			execution = append(execution, t.Clone())
		}
	})
	return execution
}

func (ds *InMemoryDatastore) GetJobByID(ctx context.Context, id string) (*tork.Job, error) {
	j, ok := ds.jobs.Get(id)
	if !ok {
		return nil, datastore.ErrJobNotFound
	}
	j = j.Clone()
	execution := ds.getExecution(id)
	sort.Slice(execution, func(i, j int) bool {
		posi := execution[i].Position
		posj := execution[j].Position
		if posi != posj {
			return posi < posj
		}
		ci := execution[i].StartedAt
		cj := execution[j].StartedAt
		if ci == nil {
			return true
		}
		if cj == nil {
			return false
		}
		return ci.Before(*cj)
	})
	j.Execution = execution
	return j.Clone(), nil
}

func (ds *InMemoryDatastore) GetActiveTasks(ctx context.Context, jobID string) ([]*tork.Task, error) {
	result := make([]*tork.Task, 0)
	ds.tasks.Iterate(func(_ string, t *tork.Task) {
		if t.JobID == jobID && t.State.IsActive() {
			result = append(result, t.Clone())
		}
	})
	return result, nil
}

func (ds *InMemoryDatastore) GetNextTask(ctx context.Context, parentTaskID string) (*tork.Task, error) {
	result := ds.tasks.List(func(v *tork.Task) bool {
		return v.ParentID == parentTaskID && v.State == tork.TaskStateCreated
	})
	if len(result) == 0 {
		return nil, datastore.ErrTaskNotFound
	}
	return result[0], nil
}

func (ds *InMemoryDatastore) GetJobs(ctx context.Context, currentUser, q string, page, size int) (*datastore.Page[*tork.JobSummary], error) {
	var urs []*tork.Role
	var user *tork.User
	if currentUser != "" {
		u, err := ds.GetUser(ctx, currentUser)
		if err != nil {
			return nil, err
		}
		user = u
		ur, err := ds.GetUserRoles(ctx, u.ID)
		if err != nil {
			return nil, err
		}
		urs = ur
	}

	searchTerm, tags := parseQuery(q)
	offset := (page - 1) * size
	filtered := make([]*tork.Job, 0)
	hasPermission := func(user *tork.User, uroles []*tork.Role, job *tork.Job) bool {
		if len(job.Permissions) == 0 {
			return true
		}
		for _, p := range job.Permissions {
			if p.User != nil && p.User.Username == user.Username {
				return true
			}
			if p.Role != nil {
				for _, ur := range uroles {
					if p.Role.Slug == ur.Slug {
						return true
					}
				}
			}
		}
		return false
	}
	ds.jobs.Iterate(func(_ string, j *tork.Job) {
		if currentUser != "" && !hasPermission(user, urs, j) {
			return
		}
		if searchTerm != "" {
			if strings.Contains(strings.ToLower(j.Name), strings.ToLower(searchTerm)) ||
				strings.Contains(strings.ToLower(string(j.State)), strings.ToLower(searchTerm)) {
				filtered = append(filtered, j)
			}
		} else if len(tags) > 0 {
			if slices.Intersect(j.Tags, tags) {
				filtered = append(filtered, j)
			}
		} else {
			filtered = append(filtered, j)
		}
	})
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
	})
	result := make([]*tork.JobSummary, 0)
	for i := offset; i < (offset+size) && i < len(filtered); i++ {
		j := filtered[i]
		result = append(result, tork.NewJobSummary(j))
	}
	totalPages := len(filtered) / size
	if len(filtered)%size != 0 {
		totalPages = totalPages + 1
	}
	return &datastore.Page[*tork.JobSummary]{
		Items:      result,
		Number:     page,
		Size:       len(result),
		TotalPages: totalPages,
		TotalItems: len(filtered),
	}, nil
}

func (ds *InMemoryDatastore) CreateTaskLogPart(ctx context.Context, p *tork.TaskLogPart) error {
	if p.TaskID == "" {
		return errors.Errorf("must provide task id")
	}
	if p.Number < 1 {
		return errors.Errorf("part number must be > 0")
	}
	now := time.Now().UTC()
	p.CreatedAt = &now
	ds.logsMu.Lock()
	defer ds.logsMu.Unlock()
	logs, ok := ds.logs.Get(p.TaskID)
	if !ok {
		logs = make([]*tork.TaskLogPart, 0)
	}
	logs = append(logs, p)
	ds.logs.Set(p.TaskID, logs)
	return nil
}

func (ds *InMemoryDatastore) GetTaskLogParts(ctx context.Context, taskID, q string, page, size int) (*datastore.Page[*tork.TaskLogPart], error) {
	_, err := ds.GetTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}
	parts, ok := ds.logs.Get(taskID)
	if !ok {
		return &datastore.Page[*tork.TaskLogPart]{
			Items:      make([]*tork.TaskLogPart, 0),
			Number:     1,
			Size:       0,
			TotalPages: 0,
			TotalItems: 0,
		}, nil
	}

	var filteredLogs []*tork.TaskLogPart
	if q == "" {
		filteredLogs = parts
	} else {
		searchTerm, _ := parseQuery(q)
		for _, p := range parts {
			if strings.Contains(p.Contents, searchTerm) {
				filteredLogs = append(filteredLogs, p)
			}
		}
	}

	sort.Slice(filteredLogs, func(i, j int) bool {
		return filteredLogs[i].Number > filteredLogs[j].Number
	})
	offset := (page - 1) * size
	result := make([]*tork.TaskLogPart, 0)
	for i := offset; i < (offset+size) && i < len(filteredLogs); i++ {
		p := filteredLogs[i]
		result = append(result, p)
	}
	totalPages := len(filteredLogs) / size
	if len(filteredLogs)%size != 0 {
		totalPages = totalPages + 1
	}
	return &datastore.Page[*tork.TaskLogPart]{
		Items:      result,
		Number:     page,
		Size:       len(result),
		TotalPages: totalPages,
		TotalItems: len(filteredLogs),
	}, nil
}

func (ds *InMemoryDatastore) GetJobLogParts(ctx context.Context, jobID, q string, page, size int) (*datastore.Page[*tork.TaskLogPart], error) {
	execution := ds.getExecution(jobID)
	allParts := make([]*tork.TaskLogPart, 0)
	for _, task := range execution {
		parts, ok := ds.logs.Get(task.ID)
		if ok {
			allParts = append(allParts, parts...)
		}
	}
	if (len(allParts)) == 0 {
		return &datastore.Page[*tork.TaskLogPart]{
			Items:      make([]*tork.TaskLogPart, 0),
			Number:     1,
			Size:       0,
			TotalPages: 0,
			TotalItems: 0,
		}, nil
	}

	var filtered []*tork.TaskLogPart
	if q == "" {
		filtered = allParts
	} else {
		searchTerm, _ := parseQuery(q)
		for _, p := range allParts {
			if strings.Contains(p.Contents, searchTerm) {
				filtered = append(filtered, p)
			}
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		ti, _ := ds.tasks.Get(filtered[i].TaskID)
		tj, _ := ds.tasks.Get(filtered[j].TaskID)
		if ti.Position > tj.Position {
			return true
		} else if ti.Position < tj.Position {
			return false
		}
		return filtered[i].Number > filtered[j].Number
	})
	offset := (page - 1) * size
	result := make([]*tork.TaskLogPart, 0)
	for i := offset; i < (offset+size) && i < len(filtered); i++ {
		p := filtered[i]
		result = append(result, p)
	}
	totalPages := len(filtered) / size
	if len(filtered)%size != 0 {
		totalPages = totalPages + 1
	}
	return &datastore.Page[*tork.TaskLogPart]{
		Items:      result,
		Number:     page,
		Size:       len(result),
		TotalPages: totalPages,
		TotalItems: len(filtered),
	}, nil
}

func (ds *InMemoryDatastore) GetMetrics(ctx context.Context) (*tork.Metrics, error) {
	s := &tork.Metrics{}

	ds.jobs.Iterate(func(_ string, j *tork.Job) {
		if j.State == tork.JobStateRunning {
			s.Jobs.Running = s.Jobs.Running + 1
		}
	})

	ds.tasks.Iterate(func(_ string, t *tork.Task) {
		if t.State == tork.TaskStateRunning {
			s.Tasks.Running = s.Tasks.Running + 1
		}
	})

	ds.nodes.Iterate(func(_ string, n *tork.Node) {
		if n.LastHeartbeatAt.After(time.Now().UTC().Add(-(time.Minute * 5))) {
			s.Nodes.Running = s.Nodes.Running + 1
			s.Nodes.CPUPercent = s.Nodes.CPUPercent + n.CPUPercent
		}
	})
	// calculate average
	if s.Nodes.Running > 0 {
		s.Nodes.CPUPercent = s.Nodes.CPUPercent / float64(s.Nodes.Running)
	}

	return s, nil
}

func (ds *InMemoryDatastore) GetUser(ctx context.Context, uid string) (*tork.User, error) {
	if uid == tork.USER_GUEST {
		return guestUser, nil
	}
	user, ok := ds.usersByID.Get(uid)
	if ok {
		return user, nil
	}
	user, ok = ds.usersByUsername.Get(uid)
	if ok {
		return user, nil
	}
	return nil, datastore.ErrUserNotFound
}

func (ds *InMemoryDatastore) GetRole(ctx context.Context, id string) (*tork.Role, error) {
	var r *tork.Role
	ds.roles.Iterate(func(key string, v *tork.Role) {
		if key == id || v.Slug == id {
			r = v
		}
	})
	if r == nil {
		return nil, datastore.ErrRoleNotFound
	}
	return r, nil
}

func (ds *InMemoryDatastore) CreateUser(ctx context.Context, u *tork.User) error {
	if _, ok := ds.usersByID.Get(u.ID); ok {
		return errors.New("user already exists")
	}
	if _, ok := ds.usersByUsername.Get(u.Username); ok {
		return errors.New("user already exists")
	}
	ds.usersByID.Set(u.ID, u)
	ds.usersByUsername.Set(u.Username, u)
	return nil
}

func (ds *InMemoryDatastore) CreateRole(ctx context.Context, r *tork.Role) error {
	r.ID = uuid.NewUUID()
	now := time.Now().UTC()
	r.CreatedAt = &now
	ds.roles.Set(r.ID, r)
	return nil
}

func (ds *InMemoryDatastore) GetRoles(ctx context.Context) ([]*tork.Role, error) {
	roles := make([]*tork.Role, 0)
	ds.roles.Iterate(func(_ string, v *tork.Role) {
		roles = append(roles, v)
	})
	sort.Slice(roles, func(i, j int) bool {
		return roles[i].Name < roles[j].Name
	})
	return roles, nil
}

func (ds *InMemoryDatastore) AssignRole(ctx context.Context, userID, roleID string) error {
	now := time.Now().UTC()
	ur := &tork.UserRole{
		ID:        uuid.NewUUID(),
		UserID:    userID,
		RoleID:    roleID,
		CreatedAt: &now,
	}
	urs, ok := ds.userRoles.Get(userID)
	if !ok {
		urs = make([]*tork.UserRole, 0)
	}
	urs = append(urs, ur)
	ds.userRoles.Set(userID, urs)
	return nil
}

func (ds *InMemoryDatastore) UnassignRole(ctx context.Context, userID, roleID string) error {
	urs, ok := ds.userRoles.Get(userID)
	if !ok {
		return nil
	}
	nurs := make([]*tork.UserRole, 0)
	for _, v := range urs {
		if v.UserID != userID || v.RoleID != roleID {
			nurs = append(nurs, v)
		}
	}
	ds.userRoles.Set(userID, nurs)
	return nil
}

func (ds *InMemoryDatastore) GetUserRoles(ctx context.Context, userID string) ([]*tork.Role, error) {
	uroles, ok := ds.userRoles.Get(userID)
	if !ok {
		return make([]*tork.Role, 0), nil
	}
	result := make([]*tork.Role, len(uroles))
	for i, ur := range uroles {
		r, ok := ds.roles.Get(ur.RoleID)
		if !ok {
			return nil, datastore.ErrRoleNotFound
		}
		result[i] = r
	}
	return result, nil
}

func (ds *InMemoryDatastore) WithTx(ctx context.Context, f func(tx datastore.Datastore) error) error {
	return f(ds)
}

func (ds *InMemoryDatastore) onJobEviction(s string, job *tork.Job) {
	tasks := make([]*tork.Task, 0)
	ds.tasks.Iterate(func(_ string, t *tork.Task) {
		if t.JobID == job.ID {
			tasks = append(tasks, t.Clone())
		}
	})
	for _, task := range tasks {
		ds.tasks.Delete(task.ID)
	}
}

func (ds *InMemoryDatastore) HealthCheck(ctx context.Context) error {
	return nil
}

func parseQuery(query string) (string, []string) {
	terms := []string{}
	tags := []string{}
	parts := strings.Fields(query)
	for _, part := range parts {
		if strings.HasPrefix(part, "tag:") {
			tags = append(tags, strings.TrimPrefix(part, "tag:"))
		} else if strings.HasPrefix(part, "tags:") {
			tags = append(tags, strings.Split(strings.TrimPrefix(part, "tag:"), ",")...)
		} else {
			terms = append(terms, part)
		}
	}
	return strings.Join(terms, " "), tags
}
