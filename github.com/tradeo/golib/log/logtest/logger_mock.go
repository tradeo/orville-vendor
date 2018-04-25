package logtest

import (
	"bytes"
	"fmt"
)

// LoggerMock mocks a Logger interface.
type LoggerMock struct {
	Infos  []string
	Errors []string
}

// NewLoggerMock creates MockLogger instance.
func NewLoggerMock() *LoggerMock {
	return &LoggerMock{
		Infos:  make([]string, 0),
		Errors: make([]string, 0),
	}
}

// Info adds the log to the Infos slice
func (l *LoggerMock) Info(args ...interface{}) {
	l.Infos = append(l.Infos, fmt.Sprint(args...))
}

// Error adds the log to the Errors slice
func (l *LoggerMock) Error(args ...interface{}) {
	l.Errors = append(l.Errors, fmt.Sprint(args...))
}

// Infof adds the log to the Infos slice
func (l *LoggerMock) Infof(format string, v ...interface{}) {
	l.Infos = append(l.Infos, fmt.Sprintf(format, v...))
}

// Errorf adds the log to the Errors slice
func (l *LoggerMock) Errorf(format string, v ...interface{}) {
	l.Errors = append(l.Errors, fmt.Sprintf(format, v...))
}

// Infow adds structured log to the Infos slice
func (l *LoggerMock) Infow(msg string, keysAndValues ...interface{}) {
	l.Infos = append(l.Infos, structuredLog(msg, keysAndValues...))
}

// Errorw adds structured log to the Errors slice
func (l *LoggerMock) Errorw(msg string, keysAndValues ...interface{}) {
	l.Errors = append(l.Errors, structuredLog(msg, keysAndValues...))
}

func structuredLog(msg string, keysAndValues ...interface{}) string {
	var buf bytes.Buffer
	buf.WriteString(msg)

	for i := 0; i <= len(keysAndValues)-1; i += 2 {
		buf.WriteString(" ")
		buf.WriteString(keysAndValues[i].(string))
		buf.WriteString("=")
		buf.WriteString(fmt.Sprintf("%+v", keysAndValues[i+1]))
	}

	return buf.String()
}

// Reset sets counters to zero
func (l *LoggerMock) Reset() {
	l.Infos = make([]string, 0)
	l.Errors = make([]string, 0)
}
