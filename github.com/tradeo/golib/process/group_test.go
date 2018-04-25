package process

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testProcess struct {
	RunCnt     int
	RunCalled  chan int
	RunUntil   chan error
	StopCalled chan int
	StopError  error
}

func newTestProcess(stopError error) *testProcess {
	return &testProcess{
		RunCalled:  make(chan int, 10),
		RunUntil:   make(chan error, 1),
		StopCalled: make(chan int, 10),
		StopError:  stopError,
	}
}

func (p *testProcess) Run() error {
	p.RunCalled <- 1
	p.RunCnt++
	err := <-p.RunUntil

	return err
}

func (p *testProcess) Stop() error {
	p.RunUntil <- nil
	return p.StopError
}

func TestGroup(t *testing.T) {
	group := NewGroup()
	foo := newTestProcess(nil)
	bar := newTestProcess(nil)

	group.AddAndRun(foo, bar)

	<-foo.RunCalled
	foo.RunUntil <- nil
	<-bar.RunCalled
	bar.RunUntil <- nil

	group.Wait()
}

func TestGroupStopOnError(t *testing.T) {
	group := NewGroup()
	foo := newTestProcess(assert.AnError)
	bar := newTestProcess(nil)

	group.AddAndRun(foo, bar)

	<-foo.RunCalled
	<-bar.RunCalled

	go func() {
		foo.RunUntil <- assert.AnError
	}()

	err := group.Wait()
	assert.EqualError(t, err, assert.AnError.Error())
}

func TestGroupStopOnShutdown(t *testing.T) {
	group := NewGroup()
	foo := newTestProcess(nil)
	bar := newTestProcess(nil)

	group.AddAndRun(foo, bar)

	<-foo.RunCalled
	<-bar.RunCalled

	go func() {
		foo.RunUntil <- ErrShutdown
	}()

	// Shutdown should not produce an error, it just have to stop all processes.
	err := group.Wait()
	assert.NoError(t, err)
}

func TestGroupStopAll(t *testing.T) {
	group := NewGroup()
	foo := newTestProcess(assert.AnError)
	bar := newTestProcess(nil)

	group.AddAndRun(foo, bar)

	<-foo.RunCalled
	<-bar.RunCalled

	group.Stop()
	group.Wait()
	group.Stop()

	group.AddAndRun(foo)
	group.AddAndRun(foo)

	group.Wait()
	assert.Equal(t, foo.RunCnt, 1)
	assert.Equal(t, bar.RunCnt, 1)
}
