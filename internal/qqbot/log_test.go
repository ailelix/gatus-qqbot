package qqbot

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestSlogAdapterAlwaysDropsBotGoDebugLogs(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	adapter := SlogAdapter{Logger: logger}
	adapter.Debugf("credentials: %s", "top-secret")
	adapter.Infof("request body: %s", "private-alert")
	adapter.Warnf("rate limited")

	if strings.Contains(output.String(), "top-secret") || strings.Contains(output.String(), "private-alert") {
		t.Fatalf("sensitive SDK log was emitted: %s", output.String())
	}
	if !strings.Contains(output.String(), "rate limited") {
		t.Fatalf("warning was not logged: %s", output.String())
	}
}
