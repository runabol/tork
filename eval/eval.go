package eval

import (
	"bytes"
	"fmt"
	"regexp"

	"github.com/antonmedv/expr"
	"github.com/pkg/errors"
	"github.com/tork/job"
	"github.com/tork/task"
)

var exprMatcher = regexp.MustCompile(`{{\s*(.+?)\s*}}`)

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
	// evaluate if expr
	ifExpr, err := evaluateTemplate(t.If, c)
	if err != nil {
		return err
	}
	t.If = ifExpr
	// evaluate pre-tasks
	pres := make([]*task.Task, len(t.Pre))
	for i, pre := range t.Pre {
		if err := Evaluate(pre, c); err != nil {
			return err
		}
		pres[i] = pre
	}
	t.Pre = pres
	// evaluate post-tasks
	posts := make([]*task.Task, len(t.Post))
	for i, post := range t.Post {
		if err := Evaluate(post, c); err != nil {
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
	loc := 0
	var buf bytes.Buffer
	for _, match := range exprMatcher.FindAllStringSubmatchIndex(v, -1) {
		startTag := match[0]
		endTag := match[1]
		startExpr := match[2]
		endExpr := match[3]
		buf.WriteString(v[loc:startTag])
		ev, err := evaluateExpression(v[startExpr:endExpr], c)
		if err != nil {
			return "", err
		}
		buf.WriteString(ev)
		loc = endTag
	}
	buf.WriteString(v[loc:])
	return buf.String(), nil
}

func evaluateExpression(ex string, c job.Context) (string, error) {
	env := map[string]interface{}{
		"inputs":    c.Inputs,
		"tasks":     c.Tasks,
		"randomInt": randomInt,
		"coinflip":  coinflip,
	}
	program, err := expr.Compile(ex, expr.Env(env))
	if err != nil {
		return "", errors.Wrapf(err, "error compiling expression: %s", ex)
	}
	output, err := expr.Run(program, env)
	if err != nil {
		return "", errors.Wrapf(err, "error evaluating expression: %s", ex)
	}
	return fmt.Sprintf("%v", output), nil
}
