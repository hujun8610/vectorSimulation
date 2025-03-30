package logger

import (
	"os"
	"strings"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// 日志级别映射
var levelMap = map[string]zapcore.Level{
	"debug":  zapcore.DebugLevel,
	"info":   zapcore.InfoLevel,
	"warn":   zapcore.WarnLevel,
	"error":  zapcore.ErrorLevel,
	"dpanic": zapcore.DPanicLevel,
	"panic":  zapcore.PanicLevel,
	"fatal":  zapcore.FatalLevel,
}

// Logger 接口定义了日志器应该提供的方法
type Logger interface {
	Debug(msg string, fields ...interface{})
	Info(msg string, fields ...interface{})
	Warn(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
	Fatal(msg string, fields ...interface{})
	With(key string, value interface{}) Logger
}

// ZapLogger 实现 Logger 接口
type ZapLogger struct {
	logger *zap.SugaredLogger
}

var (
	instance *ZapLogger
	once     sync.Once
)

// NewLogger 创建或返回单例日志器实例
func NewLogger(level, format string) Logger {
	once.Do(func() {
		instance = newZapLogger(level, format)
	})
	return instance
}

// 创建新的 Zap 日志器
func newZapLogger(level, format string) *ZapLogger {
	// 解析日志级别
	zapLevel, exists := levelMap[strings.ToLower(level)]
	if !exists {
		zapLevel = zapcore.InfoLevel
	}

	// 确定编码器
	var encoder zapcore.Encoder
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder

	// 根据配置选择编码器
	if strings.ToLower(format) == "json" {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// 创建核心
	core := zapcore.NewCore(
		encoder,
		zapcore.AddSync(os.Stdout),
		zapLevel,
	)

	// 创建 logger
	logger := zap.New(
		core,
		zap.AddCaller(),
		zap.AddCallerSkip(1),
	)

	return &ZapLogger{
		logger: logger.Sugar(),
	}
}

// Debug 记录 debug 级别的日志
func (l *ZapLogger) Debug(msg string, fields ...interface{}) {
	l.logger.Debugw(msg, fields...)
}

// Info 记录 info 级别的日志
func (l *ZapLogger) Info(msg string, fields ...interface{}) {
	l.logger.Infow(msg, fields...)
}

// Warn 记录 warn 级别的日志
func (l *ZapLogger) Warn(msg string, fields ...interface{}) {
	l.logger.Warnw(msg, fields...)
}

// Error 记录 error 级别的日志
func (l *ZapLogger) Error(msg string, fields ...interface{}) {
	l.logger.Errorw(msg, fields...)
}

// Fatal 记录 fatal 级别的日志
func (l *ZapLogger) Fatal(msg string, fields ...interface{}) {
	l.logger.Fatalw(msg, fields...)
}

// With 返回一个携带额外字段的日志器
func (l *ZapLogger) With(key string, value interface{}) Logger {
	return &ZapLogger{
		logger: l.logger.With(key, value),
	}
}

// 以下是一些辅助函数，便于直接使用 (如果您倾向于全局使用)

// Debug 使用全局日志器记录 debug 级别的日志
func Debug(msg string, fields ...interface{}) {
	if instance == nil {
		instance = newZapLogger("info", "console")
	}
	instance.Debug(msg, fields...)
}

// Info 使用全局日志器记录 info 级别的日志
func Info(msg string, fields ...interface{}) {
	if instance == nil {
		instance = newZapLogger("info", "console")
	}
	instance.Info(msg, fields...)
}

// Warn 使用全局日志器记录 warn 级别的日志
func Warn(msg string, fields ...interface{}) {
	if instance == nil {
		instance = newZapLogger("info", "console")
	}
	instance.Warn(msg, fields...)
}

// Error 使用全局日志器记录 error 级别的日志
func Error(msg string, fields ...interface{}) {
	if instance == nil {
		instance = newZapLogger("info", "console")
	}
	instance.Error(msg, fields...)
}

// Fatal 使用全局日志器记录 fatal 级别的日志
func Fatal(msg string, fields ...interface{}) {
	if instance == nil {
		instance = newZapLogger("info", "console")
	}
	instance.Fatal(msg, fields...)
}

// With 返回一个携带额外字段的全局日志器
func With(key string, value interface{}) Logger {
	if instance == nil {
		instance = newZapLogger("info", "console")
	}
	return instance.With(key, value)
}
