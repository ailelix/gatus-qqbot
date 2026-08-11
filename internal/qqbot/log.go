package qqbot

import (
	"fmt"
	"log/slog"
)

// SlogAdapter routes BotGo's logger interface through the application's slog logger.
type SlogAdapter struct {
	Logger *slog.Logger
}

// BotGo logs credentials at Debug and complete request/response bodies at Info.
func (l SlogAdapter) Debug(...interface{})   {}
func (l SlogAdapter) Info(...interface{})    {}
func (l SlogAdapter) Warn(v ...interface{})  { l.logger().Warn(fmt.Sprint(v...)) }
func (l SlogAdapter) Error(v ...interface{}) { l.logger().Error(fmt.Sprint(v...)) }

func (l SlogAdapter) Debugf(string, ...interface{}) {}
func (l SlogAdapter) Infof(string, ...interface{})  {}
func (l SlogAdapter) Warnf(format string, v ...interface{}) {
	l.logger().Warn(fmt.Sprintf(format, v...))
}
func (l SlogAdapter) Errorf(format string, v ...interface{}) {
	l.logger().Error(fmt.Sprintf(format, v...))
}
func (l SlogAdapter) Sync() error { return nil }

func (l SlogAdapter) logger() *slog.Logger {
	if l.Logger == nil {
		return slog.Default()
	}
	return l.Logger
}
