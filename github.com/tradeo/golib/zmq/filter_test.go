package zmq

import (
	"sync"
	"testing"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
	"github.com/tradeo/golib/log"
)

const msgTimeout = time.Second * 10

func TestFilter(t *testing.T) {
	in := make(chan *Message)

	mreg := metrics.NewRegistry()
	logger := &log.NullLogger{}
	filter := NewFilter(in, 100, msgTimeout, []string{"foo", "bar"}, logger, mreg)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := filter.Run()
		assert.Nil(t, err)
	}()

	// Send some messages - the baz message will be dropped
	in <- &Message{Type: "foo", Seq: 1, Payload: "test1"}
	in <- &Message{Type: "bar", Seq: 2, Payload: "test2"}
	in <- &Message{Type: "baz", Seq: 3, Payload: "test3"}

	// close should stop the filter running, but only after the buffered
	// messages are fetched
	close(in)

	msgs := make([]*Message, 0)
	for i := 0; i < 2; i++ {
		msgs = append(msgs, <-filter.Messages())
	}

	// the filter messages channel should be closed,
	// and filter stops running
	_, ok := <-filter.Messages()
	assert.False(t, ok)

	assert.Equal(t,
		[]*Message{
			&Message{"foo", 1, "test1"},
			&Message{"bar", 2, "test2"},
		},
		msgs)

	wg.Wait()
}

func TestFilterSetTypes(t *testing.T) {
	in := make(chan *Message)

	mreg := metrics.NewRegistry()
	logger := &log.NullLogger{}
	// accepts only foo and bar messages
	filter := NewFilter(in, 100, msgTimeout, []string{"foo", "bar"}, logger, mreg)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := filter.Run()
		assert.Nil(t, err)
	}()

	// Send some messages
	in <- &Message{Type: "foo", Seq: 1, Payload: "test1"}
	in <- &Message{Type: "bar", Seq: 2, Payload: "test2"}
	in <- &Message{Type: "baz", Seq: 3, Payload: "test3"} // dropped

	// Now foo, bar and baz messages will be accepted
	err := filter.SetTypes([]string{"foo", "bar", "baz"})
	assert.Nil(t, err)

	in <- &Message{Type: "foo", Seq: 4, Payload: "test4"}
	in <- &Message{Type: "bar", Seq: 5, Payload: "test5"}
	in <- &Message{Type: "baz", Seq: 6, Payload: "test6"}
	in <- &Message{Type: "qux", Seq: 7, Payload: "test7"} // dropped

	close(in)

	msgs := make([]*Message, 0)
	for msg := range filter.Messages() {
		msgs = append(msgs, msg)
	}

	assert.Equal(t, 5, len(msgs))
	assert.EqualValues(t,
		[]*Message{
			&Message{"foo", 1, "test1"},
			&Message{"bar", 2, "test2"},
			&Message{"foo", 4, "test4"},
			&Message{"bar", 5, "test5"},
			&Message{"baz", 6, "test6"},
		},
		msgs)

	wg.Wait()
}

func TestFilterSetTypesError(t *testing.T) {
	in := make(chan *Message)

	mreg := metrics.NewRegistry()
	logger := &log.NullLogger{}
	filter := NewFilter(in, 100, msgTimeout, []string{"foo", "bar"}, logger, mreg)
	err := filter.SetTypes([]string{"foo", "bar", "baz"})
	err = filter.SetTypes([]string{"foo", "bar", "baz"})
	assert.NotNil(t, err)
}

func TestFilterBufferOverflow(t *testing.T) {
	in := make(chan *Message)

	mreg := metrics.NewRegistry()
	logger := &log.NullLogger{}
	// buffer set to 2 messages
	filter := NewFilter(in, 2, msgTimeout, []string{"foo", "bar"}, logger, mreg)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := filter.Run()
		assert.NotNil(t, err)
	}()

	// Send some messages
	in <- &Message{Type: "foo", Seq: 1, Payload: "test1"}
	in <- &Message{Type: "bar", Seq: 2, Payload: "test2"}
	in <- &Message{Type: "foo", Seq: 3, Payload: "test3"}

	wg.Wait()
}

func TestFilterInvalidSeq(t *testing.T) {
	in := make(chan *Message)

	mreg := metrics.NewRegistry()
	logger := &log.NullLogger{}
	filter := NewFilter(in, 100, msgTimeout, []string{"foo", "bar"}, logger, mreg)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := filter.Run()
		assert.EqualError(t, err, "Invalid message sequence number 4, expected 3")
	}()

	// Send some messages
	in <- &Message{Type: "foo", Seq: 1, Payload: "test1"}
	in <- &Message{Type: "bar", Seq: 2, Payload: "test2"}
	in <- &Message{Type: "foo", Seq: 4, Payload: "test3"}

	wg.Wait()
}

func TestFilterSeqWrap(t *testing.T) {
	in := make(chan *Message)

	mreg := metrics.NewRegistry()
	logger := &log.NullLogger{}
	filter := NewFilter(in, 100, msgTimeout, []string{"foo", "bar"}, logger, mreg)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := filter.Run()
		assert.NoError(t, err)
	}()

	var seq int64 = 0xFFFFFFFF - 1

	// Send some messages
	in <- &Message{Type: "foo", Seq: seq - 1, Payload: "test1"}
	in <- &Message{Type: "foo", Seq: seq, Payload: "test1"}
	in <- &Message{Type: "bar", Seq: 0, Payload: "test2"}

	// close should stop the filter running, but only after the buffered
	// messages are fetched
	close(in)

	msgs := make([]*Message, 0)
	for i := 0; i < 3; i++ {
		msgs = append(msgs, <-filter.Messages())
	}

	// the filter messages channel should be closed,
	// and filter stops running
	_, ok := <-filter.Messages()
	assert.False(t, ok)

	assert.Equal(t,
		[]*Message{
			&Message{"foo", seq - 1, "test1"},
			&Message{"foo", seq, "test1"},
			&Message{"bar", 0, "test2"},
		},
		msgs)

	wg.Wait()
}

func TestFilterInvalidSeqCustomHandler(t *testing.T) {
	in := make(chan *Message)

	var failedMsg *Message
	var expectedSeq int64
	mreg := metrics.NewRegistry()
	logger := &log.NullLogger{}
	filter := NewFilter(in, 100, msgTimeout, []string{"foo", "qux"}, logger, mreg)
	filter.OnInvalidSeq(func(nextSeq int64, msg *Message) error {
		failedMsg = msg
		expectedSeq = nextSeq
		return nil
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := filter.Run()
		assert.NoError(t, err)
	}()

	// Send some messages
	in <- &Message{Type: "foo", Seq: 1, Payload: "test1"}
	in <- &Message{Type: "bar", Seq: 2, Payload: "test2"}
	in <- &Message{Type: "baz", Seq: 4, Payload: "test3"}
	in <- &Message{Type: "qux", Seq: 5, Payload: "test4"}

	close(in)

	msgs := make([]*Message, 0)
	for msg := range filter.Messages() {
		msgs = append(msgs, msg)
	}

	wg.Wait()

	assert.Len(t, msgs, 2)
	assert.Equal(t, int64(3), expectedSeq)
	assert.Equal(t, &Message{Type: "baz", Seq: 4, Payload: "test3"}, failedMsg)
}

func TestFilterStop(t *testing.T) {
	in := make(chan *Message)

	mreg := metrics.NewRegistry()
	logger := &log.NullLogger{}
	// buffer set to 2 messages
	filter := NewFilter(in, 100, msgTimeout, []string{"foo", "bar"}, logger, mreg)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := filter.Run()
		assert.Nil(t, err)
	}()

	// Send some messages
	in <- &Message{Type: "foo", Seq: 1, Payload: "test1"}
	in <- &Message{Type: "bar", Seq: 2, Payload: "test2"}

	filter.Stop()
	assert.NotPanics(t, func() { filter.Stop() })

	wg.Wait()
}

func TestFilterWaitReceiving(t *testing.T) {
	in := make(chan *Message)

	mreg := metrics.NewRegistry()
	logger := &log.NullLogger{}
	filter := NewFilter(in, 100, msgTimeout, []string{"foo", "bar"}, logger, mreg)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := filter.Run()
		assert.Nil(t, err)
	}()

	// Send some messages
	in <- &Message{Type: "foo", Seq: 1, Payload: "test1"}
	in <- &Message{Type: "bar", Seq: 2, Payload: "test2"}

	// close should stop the filter running, but only after the buffered
	// messages are fetched
	close(in)

	msgs := make([]*Message, 0)
	for i := 0; i < 2; i++ {
		msgs = append(msgs, <-filter.Messages())
	}

	// the filter messages channel should be closed,
	// and filter stops running
	_, ok := <-filter.Messages()
	assert.False(t, ok)

	assert.Equal(t,
		[]*Message{
			&Message{"foo", 1, "test1"},
			&Message{"bar", 2, "test2"},
		},
		msgs)

	wg.Wait()
	err := filter.WaitReceiving()
	assert.NoError(t, err)
}

func TestFilterWaitReceivingTimeout(t *testing.T) {
	in := make(chan *Message)

	mreg := metrics.NewRegistry()
	logger := &log.NullLogger{}
	filter := NewFilter(in, 100, time.Nanosecond*1, []string{"foo", "bar"}, logger, mreg)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := filter.Run()
		assert.Error(t, err)
		assert.Regexp(t, "^No message received for .*", err.Error())
	}()

	err := filter.WaitReceiving()
	assert.Error(t, err)
	assert.Regexp(t, "^Waiting for the first message failed: No message received for .*", err.Error())

	wg.Wait()
}

func TestFilterWaitReceivingStoppedFilter(t *testing.T) {
	in := make(chan *Message)

	mreg := metrics.NewRegistry()
	logger := &log.NullLogger{}
	filter := NewFilter(in, 100, time.Second*30, []string{"foo", "bar"}, logger, mreg)
	ready := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(ready)
		err := filter.Run()
		assert.NoError(t, err)
	}()

	<-ready
	filter.Stop()

	err := filter.WaitReceiving()
	assert.Error(t, err)
	assert.Regexp(t, "^Waiting for the first message failed: filter stopped", err.Error())

	wg.Wait()
}
