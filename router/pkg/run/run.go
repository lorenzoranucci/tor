package run

import (
	"context"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

func NewRunner(
	canal Canal,
	handler *EventHandler,
	stateHandler StateHandler,
	stateUpdateFrequency time.Duration,
) *Runner {
	p := make(chan mysql.Position)
	handler.positionChan = p

	canal.SetEventHandler(handler)

	return &Runner{canal: canal, stateHandler: stateHandler, positionChan: p, stateUpdateFrequency: stateUpdateFrequency}
}

type Runner struct {
	canal                Canal
	stateHandler         StateHandler
	positionChan         chan mysql.Position
	stateUpdateFrequency time.Duration
}

type Canal interface {
	RunFrom(mysql.Position) error
	SetEventHandler(handler canal.EventHandler)
	Close()
}

func (r *Runner) Run() error {
	lastPosition, err := r.stateHandler.GetLastPosition()
	if err != nil {
		return err
	}

	errCh := make(chan error, 2)

	go func() {
		errCh <- r.canal.RunFrom(lastPosition)
	}()

	ctx, cf := context.WithCancel(context.Background())
	ticker := time.NewTicker(r.stateUpdateFrequency)
	go func() {
		for {
			select {
			case lastPosition = <-r.positionChan:
			case <-ticker.C:
				err := r.setLastPosition(lastPosition)
				if err != nil {
					errCh <- err
				}
			case <-ctx.Done():
				_ = r.setLastPosition(lastPosition)
				return
			}
		}
	}()

	err = <-errCh
	r.canal.Close()
	cf()
	return err
}

func (r *Runner) setLastPosition(p mysql.Position) error {
	err := r.stateHandler.SetLastPosition(p)
	if err != nil {
		return err
	}
	logrus.WithField("lastPosition", p).
		Debug("last position set")

	return nil
}
