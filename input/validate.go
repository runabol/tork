package input

import (
	"regexp"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/eval"
	"github.com/runabol/tork/mq"
)

var (
	mountPattern = regexp.MustCompile("^/[0-9a-zA-Z_/]+$")
)

func (ji Job) Validate() error {
	validate := validator.New()
	if err := validate.RegisterValidation("duration", validateDuration); err != nil {
		return err
	}
	if err := validate.RegisterValidation("queue", validateQueue); err != nil {
		return err
	}
	if err := validate.RegisterValidation("expr", validateExpr); err != nil {
		return err
	}
	validate.RegisterStructValidation(validateMount, Mount{})
	validate.RegisterStructValidation(taskInputValidation, Task{})
	return validate.Struct(ji)
}

func validateExpr(fl validator.FieldLevel) bool {
	v := fl.Field().String()
	if v == "" {
		return true
	}
	return eval.ValidExpr(v)
}

func validateMount(sl validator.StructLevel) {
	mount := sl.Current().Interface().(Mount)
	if mount.Type == "" {
		sl.ReportError(mount, "mount", "Mount", "typerequired", "")
	} else if mount.Type != tork.MountTypeBind && mount.Type != tork.MountTypeVolume {
		sl.ReportError(mount, "mount", "Mount", "invalidtype", "")
	} else if mount.Type == tork.MountTypeVolume && mount.Source != "" {
		sl.ReportError(mount, "mount", "Mount", "sourcenotempty", "")
	} else if mount.Type == tork.MountTypeBind && mount.Source == "" {
		sl.ReportError(mount, "mount", "Mount", "sourcerequired", "")
	} else if mount.Source != "" && !mountPattern.MatchString(mount.Source) {
		sl.ReportError(mount, "mount", "Mount", "invalidsource", "")
	} else if mount.Target != "" && !mountPattern.MatchString(mount.Target) {
		sl.ReportError(mount, "mount", "Mount", "invalidtarget", "")
	}
}

func validateDuration(fl validator.FieldLevel) bool {
	v := fl.Field().String()
	if v == "" {
		return true
	}
	_, err := time.ParseDuration(v)
	return err == nil
}

func validateQueue(fl validator.FieldLevel) bool {
	v := fl.Field().String()
	if v == "" {
		return true
	}
	if strings.HasPrefix(v, mq.QUEUE_EXCLUSIVE_PREFIX) {
		return false
	}
	if mq.IsCoordinatorQueue(v) {
		return false
	}
	return true
}

func taskInputValidation(sl validator.StructLevel) {
	taskTypeValidation(sl)
	regularTaskValidation(sl)
	compositeTaskValidation(sl)
}

func taskTypeValidation(sl validator.StructLevel) {
	ti := sl.Current().Interface().(Task)

	if ti.Parallel != nil && ti.Each != nil {
		sl.ReportError(ti.Each, "each", "Each", "paralleloreach", "")
		sl.ReportError(ti.Parallel, "parallel", "Parallel", "paralleloreach", "")
	}

	if ti.Parallel != nil && ti.SubJob != nil {
		sl.ReportError(ti.Each, "subjob", "SubJob", "parallelorsubjob", "")
		sl.ReportError(ti.Parallel, "parallel", "Parallel", "parallelorsubjob", "")
	}

	if ti.Each != nil && ti.SubJob != nil {
		sl.ReportError(ti.Each, "subjob", "SubJob", "eachorsubjob", "")
		sl.ReportError(ti.Parallel, "each", "Each", "eachorsubjob", "")
	}

}

func compositeTaskValidation(sl validator.StructLevel) {
	t := sl.Current().Interface().(Task)
	if t.Parallel == nil && t.Each == nil && t.SubJob == nil {
		return
	}
	if t.Image != "" {
		sl.ReportError(t.Parallel, "image", "Image", "invalidcompositetask", "")
	}
	if len(t.CMD) > 0 {
		sl.ReportError(t.Parallel, "cmd", "CMD", "invalidcompositetask", "")
	}
	if len(t.Entrypoint) > 0 {
		sl.ReportError(t.Entrypoint, "entrypoint", "Entrypoint", "invalidcompositetask", "")
	}
	if t.Run != "" {
		sl.ReportError(t.Run, "run", "Run", "invalidcompositetask", "")
	}
	if len(t.Env) > 0 {
		sl.ReportError(t.Env, "env", "Env", "invalidcompositetask", "")
	}
	if t.Queue != "" {
		sl.ReportError(t.Queue, "queue", "Queue", "invalidcompositetask", "")
	}
	if len(t.Pre) > 0 {
		sl.ReportError(t.Pre, "pre", "Pre", "invalidcompositetask", "")
	}
	if len(t.Post) > 0 {
		sl.ReportError(t.Post, "post", "Post", "invalidcompositetask", "")
	}
	if len(t.Mounts) > 0 {
		sl.ReportError(t.Mounts, "mounts", "Mounts", "invalidcompositetask", "")
	}
	if t.Retry != nil {
		sl.ReportError(t.Retry, "retry", "Retry", "invalidcompositetask", "")
	}
	if t.Limits != nil {
		sl.ReportError(t.Limits, "limits", "Limits", "invalidcompositetask", "")
	}
	if t.Timeout != "" {
		sl.ReportError(t.Timeout, "timeout", "Timeout", "invalidcompositetask", "")
	}
}

func regularTaskValidation(sl validator.StructLevel) {
	t := sl.Current().Interface().(Task)
	if t.Parallel != nil || t.Each != nil || t.SubJob != nil {
		return
	}
	if t.Image == "" {
		sl.ReportError(t.Image, "image", "Image", "required", "")
	}
}
