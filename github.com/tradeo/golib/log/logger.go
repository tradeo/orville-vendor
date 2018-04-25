package log

import (
	"io"
)

// Logger interface
type Logger interface {
	Info(args ...interface{})
	Error(args ...interface{})

	Infof(format string, v ...interface{})
	Errorf(format string, v ...interface{})

	Infow(msg string, keysAndValues ...interface{})
	Errorw(msg string, keysAndValues ...interface{})
}

// SyncLogger can flushes any buffered log entries.
type SyncLogger interface {
	Logger
	Sync() error
}

// NullLogger does not do any logging
type NullLogger struct {
}

// Info does nothing
func (l *NullLogger) Info(args ...interface{}) {}

// Error does nothing
func (l *NullLogger) Error(args ...interface{}) {}

// Infof does nothing
func (l *NullLogger) Infof(format string, v ...interface{}) {}

// Errorf does nothing
func (l *NullLogger) Errorf(format string, v ...interface{}) {}

// Infow does nothing
func (l *NullLogger) Infow(msg string, keysAndValues ...interface{}) {}

// Errorw does nothing
func (l *NullLogger) Errorw(msg string, keysAndValues ...interface{}) {}

// InfoWriter is implementation of the io.Writer interface.
type InfoWriter struct {
	logger Logger
}

// NewInfoWriter creates new info writer.
func NewInfoWriter(l Logger) io.Writer {
	return &InfoWriter{l}
}

func (w *InfoWriter) Write(p []byte) (n int, err error) {
	w.logger.Infof("%s", p)
	return len(p), nil
}
