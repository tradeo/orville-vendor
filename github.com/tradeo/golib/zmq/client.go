package zmq

import (
	"fmt"
	"math/rand"
	"sync/atomic"

	zmq "github.com/pebbe/zmq3"
	"github.com/tradeo/golib/log"
)

// Message is a JSON message.
type Message struct {
	Type    string
	Seq     int64
	Payload interface{}
}

// Parser parses ZMQ messages payload
type Parser interface {
	Parse(data string) (*Message, error)
}

// Client listens on a zmq socket and parses the messages with a given Parser.
//
// The parsed messages can be fetched from a channel returned by Messages().
type Client struct {
	sock          *zmq.Socket
	closeSock     *zmq.Socket
	closeSockName string
	parser        Parser
	endpoint      string
	logger        log.Logger
	messages      chan *Message
	stopped       uint32
	done          chan struct{}
}

// Generates random zmq inproc socket name.
func genZmqInprocName() string {
	var s string
	for i := 0; i < 16; i++ {
		s += string('a' + rand.Int63n(25))
	}

	return "inproc://" + s
}

// NewClient creates Client instances.
func NewClient(
	endpoint string,
	parser Parser,
	logger log.Logger,
) (*Client, error) {
	client := &Client{
		stopped:       0,
		closeSockName: genZmqInprocName(),
		endpoint:      endpoint,
		parser:        parser,
		logger:        logger,
		messages:      make(chan *Message),
		done:          make(chan struct{}),
	}

	var err error
	defer func() {
		if err == nil {
			return
		}

		if client.sock != nil {
			client.sock.Close()
		}

		if client.closeSock != nil {
			client.closeSock.Close()
		}
	}()

	client.sock, err = newSubSock(endpoint)
	if err != nil {
		return nil, fmt.Errorf("Could not connect ZMQ socket: %s", err)
	}

	client.closeSock, err = client.newCloseSock()
	if err != nil {
		return nil, fmt.Errorf("Could not create ZMQ close inproc socket: %s", err)
	}

	return client, nil
}

// Creates Zmq sub socket
func newSubSock(endpoint string) (*zmq.Socket, error) {
	sub, _ := zmq.NewSocket(zmq.SUB)
	if err := sub.Connect(endpoint); err != nil {
		sub.Close()
		return nil, err
	}
	sub.SetSubscribe("")
	return sub, nil
}

// Creates new close socket used for breaking the poll loop in Run().
func (zc *Client) newCloseSock() (*zmq.Socket, error) {
	sub, _ := zmq.NewSocket(zmq.PAIR)
	if err := sub.Bind(zc.closeSockName); err != nil {
		sub.Close()
		return nil, err
	}
	return sub, nil
}

// Receive data from the zmq socket and send the messages
// over the messages channel.
func (zc *Client) recv() error {
	data, err := zc.sock.Recv(0)
	if err != nil {
		return err
	}

	msg, err := zc.parser.Parse(data)
	if err != nil {
		zc.logger.Errorf("error parsing zmq msg '%s': %v", data, err)
		return err
	}

	if msg == nil {
		return nil
	}

	select {
	case zc.messages <- msg:
	case <-zc.done:
	}

	return nil
}

// Messages returns the zmq messages channel
func (zc *Client) Messages() chan *Message {
	return zc.messages
}

// Stop stops the processing.
//
// Sends message on the closeSocket to the poll loop in Run().
func (zc *Client) Stop() error {
	if atomic.LoadUint32(&zc.stopped) != 0 {
		return nil
	}
	atomic.StoreUint32(&zc.stopped, 1)

	sock, _ := zmq.NewSocket(zmq.PAIR)
	defer sock.Close()
	if err := sock.Connect(zc.closeSockName); err != nil {
		return fmt.Errorf("Could not connect to zmq close socket: %s", err)
	}

	close(zc.done)
	sock.Send("close", 0)
	return nil
}

// Run listens on the ZMQ socket.
func (zc *Client) Run() error {
	defer func() {
		atomic.StoreUint32(&zc.stopped, 1)
		zc.sock.Close()
		zc.closeSock.Close()
		close(zc.messages)
	}()

	zc.logger.Info("ZMQ Client start")
	poller := zmq.NewPoller()
	poller.Add(zc.sock, zmq.POLLIN)
	poller.Add(zc.closeSock, zmq.POLLIN)

	zc.logger.Infof("Listening for ØMQ messages on %s", zc.endpoint)

	for {
		sockets, _ := poller.Poll(-1)
		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case zc.sock:
				err := zc.recv()
				if err != nil {
					zc.logger.Error(err.Error())
					return err
				}
			case zc.closeSock:
				zc.logger.Info("ZMQ Client shutdown")
				return nil
			}
		}
	}
}
