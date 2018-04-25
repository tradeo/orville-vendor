package log

import (
	"go.uber.org/zap"
)

// NewProductionZapLogger creates zap logger for production.
func NewProductionZapLogger(dsn, env string) (*SentryLogger, error) {
	config := zap.NewProductionConfig()
	config.DisableCaller = true
	config.DisableStacktrace = true

	zapLogger, err := config.Build()
	if err != nil {
		return nil, err
	}

	return NewSentryLogger(zapLogger, dsn, env), nil
}

// NewDevelopmentZapLogger creates zap logger for development.
func NewDevelopmentZapLogger() (*zap.SugaredLogger, error) {
	config := zap.NewDevelopmentConfig()
	config.DisableCaller = true

	logger, err := config.Build()
	if err != nil {
		return nil, err
	}

	return logger.Sugar(), nil
}
