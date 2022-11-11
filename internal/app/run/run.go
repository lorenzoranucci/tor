package run

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

func NewRunner(canal *canal.Canal) *Runner {
	return &Runner{canal: canal}
}

type Runner struct {
	canal *canal.Canal
}

func (r *Runner) Run() error {
	return r.canal.Run()
}
