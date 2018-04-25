package log

import (
	"bytes"
	"errors"

	raven "github.com/getsentry/raven-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SentryWriter sends data to sentry as error.
type SentryWriter struct {
	client *raven.Client
}

func (s *SentryWriter) Write(p []byte) (n int, err error) {
	var buf bytes.Buffer
	n, err = buf.Write(p)
	if err != nil {
		s.client.CaptureError(err, nil)
		return n, err
	}

	s.client.CaptureError(errors.New(buf.String()), nil)
	return n, nil
}

// Sync implements zapcore.WriteSyncer interface.
func (s *SentryWriter) Sync() error {
	return nil
}

// SentryLogger is a SyncLogger decorator that reports errors to Sentry.
// It can be used simultaneously from multiple goroutines.
type SentryLogger struct {
	sentry *raven.Client
	*zap.SugaredLogger
}

// NewSentryLogger creates SentryLogger instance.
func NewSentryLogger(logger *zap.Logger, dsn, env string) *SentryLogger {
	client := newSentryClient(dsn, env)
	if client == nil {
		return &SentryLogger{SugaredLogger: logger.Sugar(), sentry: nil}
	}

	jsonEncoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewTee(
		logger.Core(),
		zapcore.NewCore(jsonEncoder, &SentryWriter{client}, zap.ErrorLevel))

	logger = zap.New(core)

	return &SentryLogger{SugaredLogger: logger.Sugar(), sentry: client}
}

func newSentryClient(dsn, env string) *raven.Client {
	if dsn == "" {
		return nil
	}

	tags := map[string]string{"environment": env}
	client, err := raven.NewWithTags(dsn, tags)
	if err != nil {
		panic(err)
	}
	return client
}

// Sync implements zapcore.WriteSyncer interface.
func (l *SentryLogger) Sync() error {
	if l.sentry != nil {
		l.sentry.Wait()
	}

	return l.SugaredLogger.Sync()
}
