package zmq

import (
	"sync"
	"testing"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
	"github.com/tradeo/golib/log"
)

func TestRouter(t *testing.T) {
	in := make(chan *Message, 10)

	msgs := make([]*Message, 0)
	handler := HandlerFunc(func(m *Message) { msgs = append(msgs, m) })
	logger := &log.NullLogger{}

	router := NewRouter(in, logger, metrics.NewRegistry())
	router.Handle("foo", handler)
	router.Handle("bar", handler)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := router.Run()
		assert.Nil(t, err)
	}()

	// Send some messages - the baz message won't be processed
	in <- &Message{Type: "foo", Seq: 1, Payload: "test1"}
	in <- &Message{Type: "bar", Seq: 2, Payload: "test2"}
	in <- &Message{Type: "baz", Seq: 3, Payload: "test3"} // skipped

	close(in)

	wg.Wait()

	assert.Equal(t,
		[]*Message{
			&Message{"foo", 1, "test1"},
			&Message{"bar", 2, "test2"},
		},
		msgs)
}

func TestRouterHandlePanic(t *testing.T) {
	in := make(chan *Message, 10)

	logger := &log.NullLogger{}

	handler := HandlerFunc(func(m *Message) {})

	router := NewRouter(in, logger, metrics.NewRegistry())
	assert.Panics(t, func() { router.Handle("", handler) })
	assert.Panics(t, func() { router.Handle("foo", nil) })
}

func TestRouterStop(t *testing.T) {
	in := make(chan *Message)

	msgs := make([]*Message, 0)
	handler := HandlerFunc(func(m *Message) {
		msgs = append(msgs, m)
	})
	logger := &log.NullLogger{}

	router := NewRouter(in, logger, metrics.NewRegistry())
	router.Handle("foo", handler)
	router.Handle("bar", handler)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := router.Run()
		assert.Nil(t, err)
	}()

	in <- &Message{Type: "foo", Seq: 1, Payload: "test1"}
	in <- &Message{Type: "bar", Seq: 2, Payload: "test2"}

	router.Stop()
	assert.NotPanics(t, func() { router.Stop() })

	wg.Wait()

	assert.Equal(t,
		[]*Message{
			&Message{"foo", 1, "test1"},
			&Message{"bar", 2, "test2"},
		},
		msgs)
}
