package zmq

import (
	"sync"
	"testing"

	zmq "github.com/pebbe/zmq3"

	"github.com/stretchr/testify/assert"
	"github.com/tradeo/golib/log"
)

type testParser struct {
}

func (*testParser) Parse(msg string) (*Message, error) {
	return &Message{Type: "test", Payload: msg}, nil
}

func TestClientRun(t *testing.T) {
	pub, _ := zmq.NewSocket(zmq.PUB)
	defer pub.Close()
	if err := pub.Bind("tcp://*:9999"); err != nil {
		panic(err)
	}

	logger := &log.NullLogger{}
	client, err := NewClient("tcp://127.0.0.1:9999", &testParser{}, logger)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var clientErr error
	recvMessages := make([]*Message, 0)

	end := make(chan struct{})

	wg.Add(3)
	go func() {
		defer wg.Done()
		clientErr = client.Run()
	}()

	go func() {
		defer wg.Done()
		for x := range client.Messages() {
			recvMessages = append(recvMessages, x)
			if len(recvMessages) == 3 {
				close(end)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-end:
				err := client.Stop()
				if err != nil {
					t.Fatal(err)
				}
				return
			default:
				pub.Send("foo", 0)
				pub.Send("bar", 0)
				pub.Send("baz", 0)
			}
		}
	}()

	wg.Wait()

	assert.Contains(t, recvMessages,
		&Message{"test", 0, "foo"},
		&Message{"test", 0, "bar"},
		&Message{"test", 0, "baz"},
	)
}

func TestClientSockError(t *testing.T) {
	logger := &log.NullLogger{}
	client, err := NewClient("dummy", &testParser{}, logger)
	assert.Nil(t, client)
	assert.NotNil(t, err)
}

func TestClientStop(t *testing.T) {
	logger := &log.NullLogger{}
	client, err := NewClient("tcp://localhost:9999", &testParser{}, logger)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = client.Run()
	}()

	assert.Nil(t, client.Stop())
	assert.Nil(t, client.Stop())
	wg.Wait()

	assert.Nil(t, err)
}
