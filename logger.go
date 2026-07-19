package go_queue

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

type EmptyLogger struct{}

func (l *EmptyLogger) Print(v ...interface{})                 {}
func (l *EmptyLogger) Printf(format string, v ...interface{}) {}
func (l *EmptyLogger) Println(v ...interface{})               {}

func (l *EmptyLogger) Fatal(v ...interface{})                 {}
func (l *EmptyLogger) Fatalf(format string, v ...interface{}) {}
func (l *EmptyLogger) Fatalln(v ...interface{})               {}

func (l *EmptyLogger) Panic(v ...interface{})                 {}
func (l *EmptyLogger) Panicf(format string, v ...interface{}) {}
func (l *EmptyLogger) Panicln(v ...interface{})               {}

type machineryLogger struct {
	log     *slog.Logger
	level   slog.Level
	enabled bool
}

func newMachineryLogger(log *slog.Logger, level slog.Level, enabled bool) *machineryLogger {
	return &machineryLogger{log: log, level: level, enabled: enabled}
}

func (l *machineryLogger) Print(v ...interface{}) {
	l.write(fmt.Sprint(v...))
}

func (l *machineryLogger) Printf(format string, v ...interface{}) {
	l.write(fmt.Sprintf(format, v...))
}

func (l *machineryLogger) Println(v ...interface{}) {
	l.write(strings.TrimSuffix(fmt.Sprintln(v...), "\n"))
}

func (l *machineryLogger) Fatal(v ...interface{}) {
	l.Print(v...)
}

func (l *machineryLogger) Fatalf(format string, v ...interface{}) {
	l.Printf(format, v...)
}

func (l *machineryLogger) Fatalln(v ...interface{}) {
	l.Println(v...)
}

func (l *machineryLogger) Panic(v ...interface{}) {
	l.Print(v...)
}

func (l *machineryLogger) Panicf(format string, v ...interface{}) {
	l.Printf(format, v...)
}

func (l *machineryLogger) Panicln(v ...interface{}) {
	l.Println(v...)
}

func (l *machineryLogger) write(message string) {
	ctx := context.Background()
	if l.enabled && l.log.Enabled(ctx, l.level) {
		l.log.Log(ctx, l.level, message)
	}
}
