package run

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

func NewRunner(canal *canal.Canal, handler canal.EventHandler, stateHandler StateHandler) *Runner {
	canal.SetEventHandler(handler)

	return &Runner{canal: canal, stateHandler: stateHandler}
}

type Runner struct {
	canal        *canal.Canal
	stateHandler StateHandler
}

func (r *Runner) Run() error {
	lastPosition, err := r.stateHandler.GetLastPosition()
	if err != nil {
		return err
	}

	return r.canal.RunFrom(lastPosition)
}
