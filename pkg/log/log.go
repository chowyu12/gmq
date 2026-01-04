package log

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

var defaultLogger *slog.Logger

func init() {
	// Default to Info level
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	defaultLogger = slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

// Init initializes global log configuration
func Init(level string) {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "info":
		l = slog.LevelInfo
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: l,
	}
	// Production environments should use JSONHandler, development can use TextHandler
	defaultLogger = slog.New(slog.NewTextHandler(os.Stdout, opts))
}

func Info(msg string, args ...any) {
	defaultLogger.Info(msg, args...)
}

func Debug(msg string, args ...any) {
	defaultLogger.Debug(msg, args...)
}

func Error(msg string, args ...any) {
	defaultLogger.Error(msg, args...)
}

func Warn(msg string, args ...any) {
	defaultLogger.Warn(msg, args...)
}

// WithContext supports Context-based logging for traceability extension
func WithContext(ctx context.Context) *slog.Logger {
	return defaultLogger
}
