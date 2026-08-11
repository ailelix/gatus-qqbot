package logging

import (
	"bytes"
	"strings"
	"testing"

	"github.com/ailelix/gatus-qqbot/internal/config"
)

func TestNewSupportsTextAndJSON(t *testing.T) {
	for _, format := range []string{"text", "json"} {
		t.Run(format, func(t *testing.T) {
			var output bytes.Buffer
			logger, err := New(config.LogConfig{Level: "warn", Format: format}, &output)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			logger.Info("hidden")
			logger.Warn("visible")
			if strings.Contains(output.String(), "hidden") || !strings.Contains(output.String(), "visible") {
				t.Fatalf("unexpected log output: %s", output.String())
			}
		})
	}
}

func TestNewRejectsUnknownOptions(t *testing.T) {
	if _, err := New(config.LogConfig{Level: "trace", Format: "text"}, &bytes.Buffer{}); err == nil {
		t.Fatal("New() with trace level error = nil")
	}
	if _, err := New(config.LogConfig{Level: "info", Format: "binary"}, &bytes.Buffer{}); err == nil {
		t.Fatal("New() with binary format error = nil")
	}
}
