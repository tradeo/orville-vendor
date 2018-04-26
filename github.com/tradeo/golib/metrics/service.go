package metrics

import (
	"io"
	"sync/atomic"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/tradeo/golib/log"
)

// Service write metrics periodically.
type Service struct {
	mreg     metrics.Registry // metrics registry
	writer   io.Writer        // metrics writer
	interval time.Duration    // write interval
	logger   log.Logger       // logger
	stopped  uint32           // flag that the process is stopped
	done     chan struct{}    // done channel used for stoping the process
}

// NewService creates service instances.
func NewService(
	mreg metrics.Registry,
	interval time.Duration,
	writer io.Writer,
	logger log.Logger,
) *Service {
	return &Service{
		mreg:     mreg,
		writer:   writer,
		interval: interval,
		logger:   logger,
		stopped:  0,
		done:     make(chan struct{}),
	}
}

// Stop the process.
func (m *Service) Stop() error {
	if atomic.CompareAndSwapUint32(&m.stopped, 0, 1) {
		close(m.done)
	}

	return nil
}

// Run the process.
func (m *Service) Run() error {
	m.logger.Infof("Metrics start reporting every %+v", m.interval)
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			m.logger.Info("Metrics shutdown")
			return nil
		case <-ticker.C:
			metrics.WriteJSONOnce(m.mreg, m.writer)
		}
	}
}
