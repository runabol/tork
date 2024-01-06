package eval

import (
	"bytes"
	"fmt"
	"regexp"

	"github.com/expr-lang/expr"
	"github.com/pkg/errors"
	"github.com/runabol/tork"
)

var exprMatcher = regexp.MustCompile(`{{\s*(.+?)\s*}}`)

func EvaluateTask(t *tork.Task, c map[string]any) error {
	// evaluate name
	name, err := EvaluateTemplate(t.Name, c)
	if err != nil {
		return err
	}
	t.Name = name
	// evaluate var
	var_, err := EvaluateTemplate(t.Var, c)
	if err != nil {
		return err
	}
	t.Var = var_
	// evaluate image
	img, err := EvaluateTemplate(t.Image, c)
	if err != nil {
		return err
	}
	t.Image = img
	// evaluate queue
	q, err := EvaluateTemplate(t.Queue, c)
	if err != nil {
		return err
	}
	t.Queue = q
	// evaluate the env vars
	env := t.Env
	for k, v := range env {
		result, err := EvaluateTemplate(v, c)
		if err != nil {
			return err
		}
		env[k] = result
	}
	t.Env = env
	// evaluate if expr
	ifExpr, err := EvaluateTemplate(t.If, c)
	if err != nil {
		return err
	}
	t.If = ifExpr
	// evaluate pre-tasks
	pres := make([]*tork.Task, len(t.Pre))
	for i, pre := range t.Pre {
		if err := EvaluateTask(pre, c); err != nil {
			return err
		}
		pres[i] = pre
	}
	t.Pre = pres
	// evaluate post-tasks
	posts := make([]*tork.Task, len(t.Post))
	for i, post := range t.Post {
		if err := EvaluateTask(post, c); err != nil {
			return err
		}
		posts[i] = post
	}
	t.Post = posts
	// evaluate parallel tasks
	if t.Parallel != nil {
		parallel := make([]*tork.Task, len(t.Parallel.Tasks))
		for i, par := range t.Parallel.Tasks {
			if err := EvaluateTask(par, c); err != nil {
				return err
			}
			parallel[i] = par
		}
		t.Parallel.Tasks = parallel
	}
	// evaluate cmd
	cmd := t.CMD
	for i, v := range cmd {
		result, err := EvaluateTemplate(v, c)
		if err != nil {
			return err
		}
		cmd[i] = result
	}
	// evaluate sub-job
	if t.SubJob != nil {
		name, err := EvaluateTemplate(t.SubJob.Name, c)
		if err != nil {
			return err
		}
		t.SubJob.Name = name
		if t.SubJob.Inputs == nil {
			t.SubJob.Inputs = make(map[string]string)
		}
		for k, v := range t.SubJob.Inputs {
			result, err := EvaluateTemplate(v, c)
			if err != nil {
				return err
			}
			t.SubJob.Inputs[k] = result
		}
		for _, wh := range t.SubJob.Webhooks {
			url, err := EvaluateTemplate(wh.URL, c)
			if err != nil {
				return err
			}
			wh.URL = url
			if wh.Headers == nil {
				wh.Headers = make(map[string]string)
			}
			for k, v := range wh.Headers {
				result, err := EvaluateTemplate(v, c)
				if err != nil {
					return err
				}
				wh.Headers[k] = result
			}
		}
	}
	return nil
}

func EvaluateTemplate(ex string, c map[string]any) (string, error) {
	if ex == "" {
		return "", nil
	}
	loc := 0
	var buf bytes.Buffer
	for _, match := range exprMatcher.FindAllStringSubmatchIndex(ex, -1) {
		startTag := match[0]
		endTag := match[1]
		startExpr := match[2]
		endExpr := match[3]
		buf.WriteString(ex[loc:startTag])
		ev, err := EvaluateExpr(ex[startExpr:endExpr], c)
		if err != nil {
			return "", err
		}
		buf.WriteString(fmt.Sprintf("%v", ev))
		loc = endTag
	}
	buf.WriteString(ex[loc:])
	return buf.String(), nil
}

func ValidExpr(ex string) bool {
	ex = sanitizeExpr(ex)
	_, err := expr.Compile(ex)
	return err == nil
}

func sanitizeExpr(ex string) string {
	if matches := exprMatcher.FindStringSubmatch(ex); matches != nil {
		return matches[1]
	}
	return ex
}

func EvaluateExpr(ex string, c map[string]any) (any, error) {
	ex = sanitizeExpr(ex)
	env := map[string]any{
		"randomInt": randomInt,
		"sequence":  sequence,
	}
	for k, v := range c {
		env[k] = v
	}
	program, err := expr.Compile(ex, expr.Env(env))
	if err != nil {
		return "", errors.Wrapf(err, "error compiling expression: %s", ex)
	}
	output, err := expr.Run(program, env)
	if err != nil {
		return "", errors.Wrapf(err, "error evaluating expression: %s", ex)
	}
	return output, nil
}
