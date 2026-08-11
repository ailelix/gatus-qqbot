package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadAppliesDefaults(t *testing.T) {
	filename := writeConfig(t, `
[qq]
app_id = "app-id"
app_secret = "secret"

[[qq.targets]]
type = "group"
id = "group-openid"
`)

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if err := cfg.ValidateServe(); err != nil {
		t.Fatalf("ValidateServe() error = %v", err)
	}
	if got, want := cfg.ListenAddress(), "127.0.0.1:8080"; got != want {
		t.Fatalf("ListenAddress() = %q, want %q", got, want)
	}
	if got, want := cfg.Server.AlertPath, "/api/v1/gatus/alerts"; got != want {
		t.Fatalf("AlertPath = %q, want %q", got, want)
	}
	if got, err := cfg.RequestTimeout(); err != nil || got != 10*time.Second {
		t.Fatalf("RequestTimeout() = %v, %v", got, err)
	}
	if got, err := cfg.DeliveryTimeout(); err != nil || got != time.Minute {
		t.Fatalf("DeliveryTimeout() = %v, %v", got, err)
	}
	if got, want := cfg.QQ.MaxPendingAlerts, 64; got != want {
		t.Fatalf("MaxPendingAlerts = %d, want %d", got, want)
	}
	if got, err := cfg.GatewayReadyTimeout(); err != nil || got != 30*time.Second {
		t.Fatalf("GatewayReadyTimeout() = %v, %v", got, err)
	}
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	filename := writeConfig(t, `
[qq]
app_id = "app-id"
app_secret = "secret"
typo = true
`)

	_, err := Load(filename)
	if err == nil || !strings.Contains(err.Error(), "qq.typo") {
		t.Fatalf("Load() error = %v, want unknown qq.typo", err)
	}
}

func TestExampleConfigRemainsValid(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "config.example.toml"))
	if err != nil {
		t.Fatalf("Load(config.example.toml) error = %v", err)
	}
	if err := cfg.ValidateServe(); err != nil {
		t.Fatalf("ValidateServe(config.example.toml) error = %v", err)
	}
}

func TestValidateServeReportsAllRelevantErrors(t *testing.T) {
	cfg := defaults()
	cfg.Server.Port = 0
	cfg.Server.AlertPath = "alerts"
	cfg.QQ.Targets = []TargetConfig{{Type: "room"}}

	err := cfg.ValidateServe()
	if err == nil {
		t.Fatal("ValidateServe() error = nil")
	}
	for _, text := range []string{"qq.app_id", "qq.app_secret", "server.port", "server.alert_path", "type must be", "id is required"} {
		if !strings.Contains(err.Error(), text) {
			t.Errorf("ValidateServe() error %q does not contain %q", err, text)
		}
	}
}

func TestValidateServeRejectsDuplicateTargets(t *testing.T) {
	cfg := defaults()
	cfg.QQ.AppID = "app-id"
	cfg.QQ.AppSecret = "secret"
	cfg.QQ.Targets = []TargetConfig{
		{Type: "user", ID: "same"},
		{Type: "user", ID: "same"},
	}

	err := cfg.ValidateServe()
	if err == nil || !strings.Contains(err.Error(), "duplicate user target") {
		t.Fatalf("ValidateServe() error = %v, want duplicate target error", err)
	}
}

func TestValidateServeRejectsServeMuxPatterns(t *testing.T) {
	for _, alertPath := range []string{
		"/", "/{name}", "/alerts/{rest...}", "/alerts with-space", "/alerts/%zz", "/alerts/\x00",
	} {
		t.Run(alertPath, func(t *testing.T) {
			cfg := defaults()
			cfg.QQ.AppID = "app-id"
			cfg.QQ.AppSecret = "secret"
			cfg.QQ.Targets = []TargetConfig{{Type: "group", ID: "group-openid"}}
			cfg.Server.AlertPath = alertPath
			if err := cfg.ValidateServe(); err == nil || !strings.Contains(err.Error(), "server.alert_path") {
				t.Fatalf("ValidateServe() error = %v, want alert path error", err)
			}
		})
	}
}

func TestValidateServeRequiresTokenOutsideLoopback(t *testing.T) {
	cfg := defaults()
	cfg.QQ.AppID = "app-id"
	cfg.QQ.AppSecret = "secret"
	cfg.QQ.Targets = []TargetConfig{{Type: "group", ID: "group-openid"}}
	for _, address := range []string{"", "0.0.0.0", "::", "192.0.2.1"} {
		cfg.Server.Address = address
		if err := cfg.ValidateServe(); err == nil || !strings.Contains(err.Error(), "server.auth_token") {
			t.Errorf("ValidateServe() with address %q error = %v, want auth token error", address, err)
		}
	}
	for _, address := range []string{"localhost", "127.0.0.1", "::1"} {
		cfg.Server.Address = address
		if err := cfg.ValidateServe(); err != nil {
			t.Errorf("ValidateServe() with loopback address %q error = %v", address, err)
		}
	}
}

func TestValidateServeRejectsSurroundingWhitespace(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{name: "app id", mutate: func(c *Config) { c.QQ.AppID = " app-id" }, want: "qq.app_id"},
		{name: "app secret", mutate: func(c *Config) { c.QQ.AppSecret = "secret " }, want: "qq.app_secret"},
		{name: "target id", mutate: func(c *Config) { c.QQ.Targets[0].ID = "group-openid " }, want: "id must not"},
		{name: "target name", mutate: func(c *Config) { c.QQ.Targets[0].Name = " ops" }, want: "name must not"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaults()
			cfg.QQ.AppID = "app-id"
			cfg.QQ.AppSecret = "secret"
			cfg.QQ.Targets = []TargetConfig{{Type: "group", ID: "group-openid"}}
			tt.mutate(&cfg)
			if err := cfg.ValidateServe(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateServe() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestValidateServeRejectsInvalidGatewayReadyTimeout(t *testing.T) {
	cfg := defaults()
	cfg.QQ.AppID = "app-id"
	cfg.QQ.AppSecret = "secret"
	cfg.QQ.Targets = []TargetConfig{{Type: "group", ID: "group-openid"}}
	cfg.QQ.GatewayReadyTimeout = "0s"
	if err := cfg.ValidateServe(); err == nil || !strings.Contains(err.Error(), "qq.gateway_ready_timeout") {
		t.Fatalf("ValidateServe() error = %v, want gateway timeout error", err)
	}
}

func TestValidateServeRejectsInvalidDeliveryTimeout(t *testing.T) {
	cfg := defaults()
	cfg.QQ.AppID = "app-id"
	cfg.QQ.AppSecret = "secret"
	cfg.QQ.Targets = []TargetConfig{{Type: "group", ID: "group-openid"}}
	cfg.QQ.DeliveryTimeout = "0s"
	if err := cfg.ValidateServe(); err == nil || !strings.Contains(err.Error(), "qq.delivery_timeout") {
		t.Fatalf("ValidateServe() error = %v, want delivery timeout error", err)
	}
}

func TestValidateServeRejectsInvalidMaxPendingAlerts(t *testing.T) {
	for _, value := range []int{-1, 10_001} {
		cfg := defaults()
		cfg.QQ.AppID = "app-id"
		cfg.QQ.AppSecret = "secret"
		cfg.QQ.Targets = []TargetConfig{{Type: "group", ID: "group-openid"}}
		cfg.QQ.MaxPendingAlerts = value
		if err := cfg.ValidateServe(); err == nil || !strings.Contains(err.Error(), "qq.max_pending_alerts") {
			t.Errorf("ValidateServe() with max pending %d error = %v, want queue limit error", value, err)
		}
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	filename := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(filename, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return filename
}
