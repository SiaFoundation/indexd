package testutils

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger creates a console logger used for testing.
func NewLogger(enable bool) *zap.Logger {
	if !enable {
		return zap.NewNop()
	}
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.StacktraceKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(config)

	return zap.New(
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zap.DebugLevel),
		zap.AddCaller(),
		zap.AddStacktrace(zap.DebugLevel),
	)
}
