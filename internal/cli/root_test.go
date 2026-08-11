package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/ailelix/gatus-qqbot/internal/config"
	"github.com/ailelix/gatus-qqbot/internal/qqbot"
)

func TestRootHelpListsCommands(t *testing.T) {
	var stdout, stderr bytes.Buffer
	command := New(&stdout, &stderr)
	command.SetArgs([]string{"--help"})
	if err := command.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, want := range []string{"auth", "serve", "--config"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("help output does not contain %q:\n%s", want, stdout.String())
		}
	}
}

func TestAuthHelpDescribesAgentBinding(t *testing.T) {
	var stdout, stderr bytes.Buffer
	command := New(&stdout, &stderr)
	command.SetArgs([]string{"auth", "--help"})
	if err := command.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout.String(), "Agent service QR code") {
		t.Fatalf("auth help does not describe Agent binding:\n%s", stdout.String())
	}
	for _, unwanted := range []string{"friend", "callback-data"} {
		if strings.Contains(stdout.String(), unwanted) {
			t.Errorf("auth help unexpectedly contains %q:\n%s", unwanted, stdout.String())
		}
	}
}

func TestWriteCredentialsProducesValidConfigSnippet(t *testing.T) {
	var output bytes.Buffer
	credentials := qqbot.QRCredentials{
		AppID:      "12345",
		AppSecret:  "secret-value",
		UserOpenID: "scanner-openid",
	}
	if err := writeCredentials(&output, "custom.toml", credentials); err != nil {
		t.Fatalf("writeCredentials() error = %v", err)
	}

	text := output.String()
	start := strings.Index(text, "[qq]")
	end := strings.Index(text, "\nScanner user_openid:")
	if start < 0 || end < 0 || end <= start {
		t.Fatalf("writeCredentials() output does not contain the expected sections:\n%s", text)
	}
	var decoded struct {
		QQ config.QQConfig `toml:"qq"`
	}
	if _, err := toml.Decode(text[start:end], &decoded); err != nil {
		t.Fatalf("decode credential snippet: %v", err)
	}
	if decoded.QQ.AppID != credentials.AppID || decoded.QQ.AppSecret != credentials.AppSecret {
		t.Fatalf("decoded credentials = %#v", decoded.QQ)
	}
	if !strings.Contains(text, "The file was not modified.") || !strings.Contains(text, `"custom.toml"`) || !strings.Contains(text, credentials.UserOpenID) {
		t.Fatalf("writeCredentials() output is incomplete:\n%s", text)
	}
}

func TestServeReportsConfigErrorWithoutUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	command := New(&stdout, &stderr)
	command.SetArgs([]string{"serve", "--config", "missing.toml"})
	err := command.Execute()
	if err == nil || !strings.Contains(err.Error(), "missing.toml") {
		t.Fatalf("Execute() error = %v", err)
	}
	if strings.Contains(stderr.String(), "Usage:") {
		t.Fatalf("stderr unexpectedly contains usage:\n%s", stderr.String())
	}
}

func TestCommandsRejectPositionalArguments(t *testing.T) {
	for _, name := range []string{"auth", "serve"} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			command := New(&stdout, &stderr)
			command.SetArgs([]string{name, "unexpected"})
			if err := command.Execute(); err == nil {
				t.Fatal("Execute() error = nil")
			}
		})
	}
}

func TestDeliveryContextOutlivesServiceUntilExplicitlyStopped(t *testing.T) {
	type contextKey string
	serviceCtx, cancelService := context.WithCancel(context.WithValue(context.Background(), contextKey("key"), "value"))
	deliveryCtx, cancelDeliveries := newDeliveryContext(serviceCtx)
	cancelService()

	if got := deliveryCtx.Value(contextKey("key")); got != "value" {
		t.Fatalf("delivery context value = %v, want value", got)
	}
	select {
	case <-deliveryCtx.Done():
		t.Fatal("service cancellation stopped deliveries before HTTP shutdown completed")
	default:
	}
	cancelDeliveries()
	select {
	case <-deliveryCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("delivery context did not stop after explicit cancellation")
	}
}
