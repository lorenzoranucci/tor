package run_test

import (
	"errors"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/lorenzoranucci/tor/router/pkg/run"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunner_RunWhenCanalRunFail(t *testing.T) {
	handler := buildEventHandler(t)

	expectedErr := errors.New("a")
	r := run.NewRunner(
		&canalMock{runFromErr: expectedErr},
		handler,
		&stateHandlerMock{},
		1*time.Millisecond,
	)

	err := r.Run()
	assert.Equal(t, err, expectedErr)
}

func TestRunner_RunWhenStateHandlerSetFail(t *testing.T) {
	handler := buildEventHandler(t)

	expectedErr := errors.New("a")
	r := run.NewRunner(
		&canalMock{},
		handler,
		&stateHandlerMock{setLastPositionErr: expectedErr},
		1*time.Millisecond,
	)

	err := r.Run()
	assert.Equal(t, err, expectedErr)
}

func TestRunner_RunWhenStateHandlerGetFail(t *testing.T) {
	handler := buildEventHandler(t)

	expectedErr := errors.New("a")
	r := run.NewRunner(
		&canalMock{},
		handler,
		&stateHandlerMock{getLastPositionErr: expectedErr},
		1*time.Millisecond,
	)

	err := r.Run()
	assert.Equal(t, err, expectedErr)
}

func TestRunner_RunWhenNoError(t *testing.T) {
	handler := buildEventHandler(t)

	r := run.NewRunner(
		&canalMock{},
		handler,
		&stateHandlerMock{},
		1*time.Millisecond,
	)

	err := r.Run()
	assert.NoError(t, err)
}

type canalMock struct {
	runFromErr error
}

func (c *canalMock) Close() {}

func (c *canalMock) RunFrom(mysql.Position) error {
	time.Sleep(time.Second * 1) // simulate listening time
	return c.runFromErr
}

func (c *canalMock) SetEventHandler(canal.EventHandler) {}

type stateHandlerMock struct {
	setLastPositionErr error
	getLastPositionErr error
}

func (s *stateHandlerMock) GetLastPosition() (mysql.Position, error) {
	return mysql.Position{}, s.getLastPositionErr
}

func (s *stateHandlerMock) SetLastPosition(mysql.Position) error {
	return s.setLastPositionErr
}

func buildEventHandler(t *testing.T) *run.EventHandler {
	eh, err := run.NewEventHandler(
		nil,
		"",
		"",
		"",
		nil,
		nil,
		false,
	)
	require.NoError(t, err)
	return eh
}
