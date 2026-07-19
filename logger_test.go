package go_queue

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestMachineryLoggerRoutesLevelsThroughSlog(t *testing.T) {
	var output bytes.Buffer
	log := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))

	newMachineryLogger(log, slog.LevelDebug, false).Print("hidden debug")
	newMachineryLogger(log, slog.LevelInfo, true).Print("worker ready")
	newMachineryLogger(log, slog.LevelWarn, true).Print("worker delayed")
	newMachineryLogger(log, slog.LevelError, true).Print("worker failed")

	text := output.String()
	if strings.Contains(text, "hidden debug") {
		t.Fatalf("debug output was not disabled: %q", text)
	}
	for _, expected := range []string{
		"level=INFO msg=\"worker ready\"",
		"level=WARN msg=\"worker delayed\"",
		"level=ERROR msg=\"worker failed\"",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("missing %q in %q", expected, text)
		}
	}
	if strings.Contains(text, "INFO:") {
		t.Fatalf("legacy Machinery formatter leaked into output: %q", text)
	}
}

func TestMachineryLoggerEnablesDebugOnlyWhenLoggerAllowsIt(t *testing.T) {
	tests := []struct {
		name       string
		level      slog.Level
		debug      bool
		wantOutput bool
	}{
		{name: "debug enabled", level: slog.LevelDebug, debug: true, wantOutput: true},
		{name: "queue debug disabled", level: slog.LevelDebug, debug: false, wantOutput: false},
		{name: "logger debug disabled", level: slog.LevelInfo, debug: true, wantOutput: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			log := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: test.level}))
			newMachineryLogger(log, slog.LevelDebug, test.debug).Print("task payload")

			if got := strings.Contains(output.String(), "task payload"); got != test.wantOutput {
				t.Fatalf("debug output presence = %t, want %t: %q", got, test.wantOutput, output.String())
			}
		})
	}
}
