package log

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

var defaultLogger *slog.Logger

func init() {
	// 默认使用 Info 级别
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	defaultLogger = slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

// Init 初始化全局日志配置
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
	// 生产环境建议使用 JSONHandler，开发环境可以用 TextHandler
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

// WithContext 支持 Context 的日志，便于链路追踪扩展
func WithContext(ctx context.Context) *slog.Logger {
	return defaultLogger
}
