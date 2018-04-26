package metrics

import (
	"sync"
	"testing"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
	"github.com/tradeo/golib/log"
)

type writerMock struct {
	received chan int
	messages []string
}

func newWriterMock() *writerMock {
	return &writerMock{
		received: make(chan int),
		messages: make([]string, 0),
	}
}

func (l *writerMock) Write(p []byte) (n int, err error) {
	if len(l.messages) == 0 {
		l.received <- 1
	}
	l.messages = append(l.messages, string(p))
	return len(p), nil
}

func TestService(t *testing.T) {
	mreg := metrics.NewRegistry()
	gauge := metrics.GetOrRegisterGauge("/test", mreg)
	gauge.Update(42)

	writer := newWriterMock()
	logger := &log.NullLogger{}
	service := NewService(mreg, time.Microsecond, writer, logger)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := service.Run()
		assert.NoError(t, err)
	}()

	<-writer.received
	service.Stop()
	wg.Wait()

	assert.NotEqual(t, 0, len(writer.messages))
	assert.Equal(t, `{"/test":{"value":42}}`+"\n", writer.messages[0])
}
