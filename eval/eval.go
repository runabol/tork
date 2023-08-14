package eval

import (
	"bytes"
	"text/template"

	"github.com/pkg/errors"
	"github.com/tork/job"
	"github.com/tork/task"
)

func Evaluate(t *task.Task, c job.Context) error {
	// evaluate name
	name, err := evaluateTemplate(t.Name, c)
	if err != nil {
		return err
	}
	t.Name = name
	// evaluate image
	img, err := evaluateTemplate(t.Image, c)
	if err != nil {
		return err
	}
	t.Image = img
	// evaluate queue
	q, err := evaluateTemplate(t.Queue, c)
	if err != nil {
		return err
	}
	t.Queue = q
	// evaluate the env vars
	env := t.Env
	for k, v := range env {
		result, err := evaluateTemplate(v, c)
		if err != nil {
			return err
		}
		env[k] = result
	}
	t.Env = env
	// evaluate pre-tasks
	pres := make([]task.Task, len(t.Pre))
	for i, pre := range t.Pre {
		if err := Evaluate(&pre, c); err != nil {
			return err
		}
		pres[i] = pre
	}
	t.Pre = pres
	// evaluate post-tasks
	posts := make([]task.Task, len(t.Post))
	for i, post := range t.Post {
		if err := Evaluate(&post, c); err != nil {
			return err
		}
		posts[i] = post
	}
	t.Post = posts
	return nil
}

func evaluateTemplate(v string, c job.Context) (string, error) {
	if v == "" {
		return "", nil
	}
	tmpl, err := template.New("").Parse(v)
	if err != nil {
		return "", errors.Wrapf(err, "invalid expression: %s", v)
	}
	var buff bytes.Buffer
	data := map[string]any{
		"inputs": c.Inputs,
		"tasks":  c.Tasks,
	}
	if err := tmpl.Execute(&buff, data); err != nil {
		return "", errors.Wrapf(err, "failed to evaluate: %s", v)
	}
	return buff.String(), nil
}
