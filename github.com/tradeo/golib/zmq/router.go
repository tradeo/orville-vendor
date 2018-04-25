package zmq

import (
	"sync/atomic"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/tradeo/golib/log"
)

const (
	msgHandleTimerMetric = "zmq/router/handle"
)

// Handler handles a message.
type Handler interface {
	Handle(*Message)
}

// HandlerFunc is adapter for the Handler inteface,
// so that regular functions can be used as handlers.
type HandlerFunc func(m *Message)

// Handle calls f(m).
func (f HandlerFunc) Handle(m *Message) {
	f(m)
}

// Router is a ZMQ message multiplexer. It matches the type of each
// incoming message against a list of registered handlers and calls the handler.
type Router struct {
	handlers map[string]Handler
	in       chan *Message
	stopped  uint32
	done     chan struct{}
	logger   log.Logger
	mreg     metrics.Registry
}

// NewRouter returns a new initialized Router.
func NewRouter(in chan *Message, logger log.Logger, mreg metrics.Registry) *Router {
	router := &Router{
		handlers: make(map[string]Handler),
		in:       in,
		stopped:  0,
		done:     make(chan struct{}),
		logger:   logger,
		mreg:     mreg,
	}

	return router
}

// Stop stops routing.
//
// Normally the routing will stop as soon as the input channel is closed.
// This func can be used in cases where the Stopping should be done explicitly.
func (r *Router) Stop() error {
	if atomic.CompareAndSwapUint32(&r.stopped, 0, 1) {
		close(r.done)
	}

	return nil
}

// Run starts routing the messages.
func (r *Router) Run() error {
	r.logger.Info("ZMQ Router start")
	msgHandleTimer := metrics.GetOrRegisterTimer(msgHandleTimerMetric, r.mreg)

	for {
		select {
		case <-r.done:
			r.logger.Info("ZMQ Router shutdown")
			return nil
		case msg, ok := <-r.in:
			if !ok {
				r.logger.Info("ZMQ Router shutdown")
				return nil
			}

			start := time.Now()
			r.handleMsg(msg)
			msgHandleTimer.UpdateSince(start)
		}
	}
}

// HandleZmq dispatches the request to the handler for the given message type.
// If a handler is not found, it's a no-op and does not return error.
func (r *Router) handleMsg(msg *Message) {
	if h, ok := r.handlers[msg.Type]; ok {
		h.Handle(msg)
	}
}

// Handle registers a handler for the given message type.
func (r *Router) Handle(msgType string, handler Handler) {
	if msgType == "" {
		panic("type is empty")
	}

	if handler == nil {
		panic("handler is nil")
	}

	r.handlers[msgType] = handler
}
