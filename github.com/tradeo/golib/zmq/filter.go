package zmq

import (
	"fmt"
	"sync/atomic"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/tradeo/golib/log"
)

const (
	// Sequence numbers range.
	// Note that the sequence numbers are wrapped,
	// and can have values between 0 and seqRange - 1.
	seqRange = 0xFFFFFFFF
	nullSeq  = -1

	queueSizeHistogramMetric = "zmq/filter/queue/size"
	msgReceivedMeterMetric   = "zmq/filter/messages/received"
)

// InvalidSeqFunc is a function that is called when the filter detects out of
// order sequence numbers.
type InvalidSeqFunc func(nextSeq int64, msg *Message) error

// Filter filters zmq messages.
//
// More specifically it reads messages from an input channel, and forwards
// only specific types of messages to an output channel. All other types
// are dropped.
//
// It also buffers the messages if the consumer of the output channel does
// not exist or is slower than input rate.
type Filter struct {
	bufferMaxSize  int             // message queue size
	msgTimeout     time.Duration   // duration with no received messages before doing timeout
	msgTypes       map[string]bool // message types that will be buffered
	msgTypesChan   chan []string   // used for setting the message types
	in             chan *Message   // incoming messages
	out            chan *Message   // filtered output
	stopped        uint32          // flag that the process is stopped
	done           chan struct{}   // done channel used for stoping the process
	receiving      chan error      // used to notify the user that the filter already received at least one message
	nextSeq        int64           // expected message sequence number
	lastInputTime  time.Time       // last time a message was received
	logger         log.Logger
	mreg           metrics.Registry
	invalidSeqFunc InvalidSeqFunc
}

// NewFilter creates Filter instances.
//
// in - Input channel. Messages are received by the input channel.
// bufferMaxSize - If the message buffer is full Run() will return with
//                 buffer overflow error.
// msgTimeout - If there are no messages received for `msgTimeout` duration,
//              Run() will return with a timeout error. If the `mstTimeout` is
//              zero Run() won't timeout at all.
// msgTypes - Message types that will be buffered, the others will be dropped.
func NewFilter(
	in chan *Message,
	bufferMaxSize int,
	msgTimeout time.Duration,
	msgTypes []string,
	logger log.Logger,
	mreg metrics.Registry,
) *Filter {
	filter := &Filter{
		bufferMaxSize: bufferMaxSize,
		msgTimeout:    msgTimeout,
		msgTypes:      make(map[string]bool),
		msgTypesChan:  make(chan []string, 1),
		in:            in,
		out:           make(chan *Message),
		stopped:       0,
		done:          make(chan struct{}),
		receiving:     make(chan error, 1),
		logger:        logger,
		mreg:          mreg,
		invalidSeqFunc: func(nextSeq int64, msg *Message) error {
			return fmt.Errorf(
				"Invalid message sequence number %d, expected %d", msg.Seq, nextSeq)
		},
	}

	filter.updateTypes(msgTypes)
	return filter
}

// OnInvalidSeq sets a invalid sequence handler.
func (f *Filter) OnInvalidSeq(isf InvalidSeqFunc) {
	f.invalidSeqFunc = isf
}

func (f *Filter) updateTypes(msgTypes []string) {
	f.msgTypes = make(map[string]bool)
	for _, v := range msgTypes {
		f.msgTypes[v] = true
	}
}

// SetTypes updates the message types that will be accepted by the filter.
func (f *Filter) SetTypes(msgTypes []string) error {
	select {
	case f.msgTypesChan <- msgTypes:
		return nil
	default:
		return fmt.Errorf("Failed to set the types")
	}
}

// WaitReceiving waits until the filter received the first message.
func (f *Filter) WaitReceiving() error {
	err, ok := <-f.receiving
	if !ok {
		return fmt.Errorf("Waiting for the first message failed: filter stopped")
	}

	if err != nil {
		return fmt.Errorf("Waiting for the first message failed: %s", err.Error())
	}

	return nil
}

// Stop stops message processing.
//
// Normally the filtering will stop as soon as the input channel is closed.
// This func can be used in cases where the Stopping should be done explicitly.
//
// Note that when the input channel is closed it processing won't stop
// until the buffered messages are sent to the output channel, while if one
// calls Stop() it will exit Run() immediately.
func (f *Filter) Stop() error {
	if atomic.CompareAndSwapUint32(&f.stopped, 0, 1) {
		close(f.done)
	}

	return nil
}

// Run starts accepting messages.
func (f *Filter) Run() error {
	defer func() {
		close(f.out)
		close(f.receiving)
	}()

	f.logger.Infof("ZMQ Filter start with message queue size %d and timeout %v", f.bufferMaxSize, f.msgTimeout)

	ticker := time.NewTicker(f.msgTimeout)
	defer ticker.Stop()

	pending := make([]*Message, 0, f.bufferMaxSize)
	hasInput := true
	f.nextSeq = nullSeq
	f.lastInputTime = time.Now()
	queueSizeMeter := metrics.GetOrRegisterHistogram(
		queueSizeHistogramMetric, f.mreg, metrics.NewExpDecaySample(1028, 0.015))
	msgReceviedMeter := metrics.GetOrRegisterMeter(msgReceivedMeterMetric, f.mreg)

	for {
		queueSizeMeter.Update(int64(len(pending)))

		if len(pending) > f.bufferMaxSize {
			err := fmt.Errorf(
				"ZMQ message buffer is full (max allowed size %d)", f.bufferMaxSize)
			f.logger.Error(err.Error())
			return err
		}

		if len(pending) > 3*f.bufferMaxSize/4 {
			f.logger.Infof(
				"ZMQ message buffer is 3/4 full (max allowed size %d)", f.bufferMaxSize)
		}

		var in chan *Message
		if hasInput {
			in = f.in
		}

		// When there is no input the loop will continue until
		// all pending messages are sent through the out channel.
		if !hasInput && len(pending) <= 0 {
			f.logger.Info("ZMQ Filter shutdown (input closed)")
			return nil
		}

		var out chan *Message
		var first *Message
		if len(pending) > 0 {
			first = pending[0]
			out = f.out
		}

		select {
		case <-f.done:
			f.logger.Info("ZMQ Filter shutdown (done triggered)")
			return nil
		case msg, ok := <-in:
			if !ok {
				hasInput = false
				continue
			}

			var err error
			pending, err = f.acceptMsg(msg, pending)
			if err != nil {
				f.logger.Info("ZMQ Filter shutdown")
				return err
			}
			msgReceviedMeter.Mark(1)
		case msgTypes := <-f.msgTypesChan:
			f.updateTypes(msgTypes)
		case out <- first:
			pending = pending[1:]
		case t := <-ticker.C:
			err := f.checkTimeout(t)
			if err != nil {
				f.logger.Info("ZMQ Filter shutdown")
				return err
			}
		}
	}
}

func (f *Filter) checkTimeout(t time.Time) error {
	if f.msgTimeout == 0 {
		return nil
	}

	if t.Sub(f.lastInputTime) <= f.msgTimeout {
		return nil
	}

	err := fmt.Errorf("No message received for %v", t.Sub(f.lastInputTime))
	if f.nextSeq == nullSeq {
		f.receiving <- err
	}

	f.logger.Error(err.Error())

	return err
}

func (f *Filter) acceptMsg(msg *Message, pending []*Message) ([]*Message, error) {
	f.lastInputTime = time.Now()

	// Alert the receiving channel that the first message is received.
	if f.nextSeq == nullSeq {
		f.receiving <- nil
	}

	err := f.checkNextSeq(msg)
	if err != nil {
		return pending, err
	}

	if _, found := f.msgTypes[msg.Type]; found {
		return append(pending, msg), nil
	}

	return pending, nil
}

// checkNextSeq checks if the message sequence number is valid.
func (f *Filter) checkNextSeq(msg *Message) error {
	if f.nextSeq != nullSeq && f.nextSeq != msg.Seq {
		err := f.invalidSeqFunc(f.nextSeq, msg)
		if err != nil {
			f.logger.Error(err.Error())
			return err
		}

		f.nextSeq = nullSeq
	}

	if f.nextSeq == nullSeq {
		f.nextSeq = msg.Seq
	}
	f.nextSeq = (f.nextSeq + 1) % seqRange

	return nil
}

// Messages returns zmq messages output channel
func (f *Filter) Messages() chan *Message {
	return f.out
}
