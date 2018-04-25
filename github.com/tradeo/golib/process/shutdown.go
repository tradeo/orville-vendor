package process

import (
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/tradeo/golib/log"
)

// ErrShutdown is an error returned by Shtudown process when the SIGTERM
// is received.
var ErrShutdown = errors.New("Shutdown signal")

// Shutdown is a process that waits for SIGTERM.
// It can be used in a process group for signalng the group to stop.
// Termination is guaranteed within given `gracePeriod`
type Shutdown struct {
	logger      log.Logger
	gracePeriod time.Duration
	sig         chan os.Signal
	once        sync.Once
	done        chan struct{}
}

// NewShutdown creates Shutdown instance.
func NewShutdown(logger log.Logger, gracePeriod time.Duration) *Shutdown {
	s := &Shutdown{
		logger:      logger,
		gracePeriod: gracePeriod,
		sig:         make(chan os.Signal, 1),
		done:        make(chan struct{}),
	}

	signal.Notify(s.sig, syscall.SIGTERM, os.Interrupt)
	return s
}

func (s *Shutdown) ensureTermination() {
	go func() {
		time.Sleep(s.gracePeriod)
		s.logger.Errorf("Shutdown: failed to shutdown within %v; exit forced", s.gracePeriod)
		os.Exit(1)
	}()
}

// Stop the process.
func (s *Shutdown) Stop() error {
	s.once.Do(func() {
		close(s.done)
	})

	return nil
}

// Run the process.
func (s *Shutdown) Run() error {
	select {
	case sig := <-s.sig:
		s.logger.Infof("Shutdown: process received signal %v", sig)
		s.ensureTermination()
		return ErrShutdown
	case <-s.done:
		s.logger.Info("Shutdown: stopped")
		s.ensureTermination()
		return nil
	}
}
