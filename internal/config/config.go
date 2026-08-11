package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	defaultPort             = 8080
	defaultAlertPath        = "/api/v1/gatus/alerts"
	defaultMaxBodyBytes     = 64 << 10
	defaultRequestTimeout   = 10 * time.Second
	defaultDeliveryTimeout  = time.Minute
	defaultMaxPendingAlerts = 64
	maxPendingAlerts        = 10_000
	defaultGatewayTimeout   = 30 * time.Second
	defaultShutdownTimeout  = 10 * time.Second
	defaultMessageMaxLength = 1800
)

// Config is the complete application configuration.
type Config struct {
	Server  ServerConfig  `toml:"server"`
	QQ      QQConfig      `toml:"qq"`
	Message MessageConfig `toml:"message"`
	Log     LogConfig     `toml:"log"`
}

type ServerConfig struct {
	Address         string `toml:"address"`
	Port            int    `toml:"port"`
	AlertPath       string `toml:"alert_path"`
	AuthToken       string `toml:"auth_token"`
	MaxBodyBytes    int64  `toml:"max_body_bytes"`
	ShutdownTimeout string `toml:"shutdown_timeout"`
}

type QQConfig struct {
	AppID               string         `toml:"app_id"`
	AppSecret           string         `toml:"app_secret"`
	RequestTimeout      string         `toml:"request_timeout"`
	DeliveryTimeout     string         `toml:"delivery_timeout"`
	MaxPendingAlerts    int            `toml:"max_pending_alerts"`
	GatewayReadyTimeout string         `toml:"gateway_ready_timeout"`
	Targets             []TargetConfig `toml:"targets"`
}

type TargetConfig struct {
	Name string `toml:"name"`
	Type string `toml:"type"`
	ID   string `toml:"id"`
}

type MessageConfig struct {
	Prefix    string `toml:"prefix"`
	MaxLength int    `toml:"max_length"`
}

type LogConfig struct {
	Level  string `toml:"level"`
	Format string `toml:"format"`
}

func Load(filename string) (Config, error) {
	cfg := defaults()
	metadata, err := toml.DecodeFile(filename, &cfg)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", filename, err)
	}
	if undecoded := metadata.Undecoded(); len(undecoded) != 0 {
		keys := make([]string, 0, len(undecoded))
		for _, key := range undecoded {
			keys = append(keys, key.String())
		}
		return Config{}, fmt.Errorf("unknown config keys: %s", strings.Join(keys, ", "))
	}
	return cfg, nil
}

func (c Config) ValidateServe() error {
	var errs []error
	if err := c.validateCredentials(); err != nil {
		errs = append(errs, err)
	}
	if strings.ContainsAny(c.Server.Address, "\r\n") {
		errs = append(errs, errors.New("server.address must not contain newlines"))
	}
	if c.Server.Address != strings.TrimSpace(c.Server.Address) {
		errs = append(errs, errors.New("server.address must not have surrounding whitespace"))
	}
	if c.Server.AuthToken != strings.TrimSpace(c.Server.AuthToken) {
		errs = append(errs, errors.New("server.auth_token must not have surrounding whitespace"))
	}
	if strings.TrimSpace(c.Server.AuthToken) == "" && !isLoopbackAddress(c.Server.Address) {
		errs = append(errs, errors.New("server.auth_token is required when server.address is not loopback"))
	}
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		errs = append(errs, errors.New("server.port must be between 1 and 65535"))
	}
	if !validHTTPPath(c.Server.AlertPath) {
		errs = append(errs, errors.New("server.alert_path must be an absolute, clean HTTP path"))
	}
	if c.Server.MaxBodyBytes < 1 {
		errs = append(errs, errors.New("server.max_body_bytes must be positive"))
	}
	if _, err := c.ShutdownTimeout(); err != nil {
		errs = append(errs, err)
	}
	if _, err := c.RequestTimeout(); err != nil {
		errs = append(errs, err)
	}
	if _, err := c.DeliveryTimeout(); err != nil {
		errs = append(errs, err)
	}
	if c.QQ.MaxPendingAlerts < 0 || c.QQ.MaxPendingAlerts > maxPendingAlerts {
		errs = append(errs, fmt.Errorf("qq.max_pending_alerts must be between 0 and %d", maxPendingAlerts))
	}
	if _, err := c.GatewayReadyTimeout(); err != nil {
		errs = append(errs, err)
	}
	if c.Message.MaxLength < 64 {
		errs = append(errs, errors.New("message.max_length must be at least 64"))
	}
	if len(c.QQ.Targets) == 0 {
		errs = append(errs, errors.New("qq.targets must contain at least one destination"))
	}
	seen := make(map[string]struct{}, len(c.QQ.Targets))
	for i, target := range c.QQ.Targets {
		if err := validateTarget(target); err != nil {
			errs = append(errs, fmt.Errorf("qq.targets[%d]: %w", i, err))
			continue
		}
		key := target.Type + "\x00" + target.ID
		if _, ok := seen[key]; ok {
			errs = append(errs, fmt.Errorf("qq.targets[%d]: duplicate %s target %q", i, target.Type, target.ID))
		}
		seen[key] = struct{}{}
	}
	if err := validateLog(c.Log); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (c Config) ListenAddress() string {
	return net.JoinHostPort(c.Server.Address, strconv.Itoa(c.Server.Port))
}

func (c Config) RequestTimeout() (time.Duration, error) {
	return positiveDuration("qq.request_timeout", c.QQ.RequestTimeout)
}

func (c Config) DeliveryTimeout() (time.Duration, error) {
	return positiveDuration("qq.delivery_timeout", c.QQ.DeliveryTimeout)
}

func (c Config) GatewayReadyTimeout() (time.Duration, error) {
	return positiveDuration("qq.gateway_ready_timeout", c.QQ.GatewayReadyTimeout)
}

func (c Config) ShutdownTimeout() (time.Duration, error) {
	return positiveDuration("server.shutdown_timeout", c.Server.ShutdownTimeout)
}

func (c Config) validateCredentials() error {
	var errs []error
	if strings.TrimSpace(c.QQ.AppID) == "" {
		errs = append(errs, errors.New("qq.app_id is required"))
	} else if c.QQ.AppID != strings.TrimSpace(c.QQ.AppID) {
		errs = append(errs, errors.New("qq.app_id must not have surrounding whitespace"))
	}
	if strings.TrimSpace(c.QQ.AppSecret) == "" {
		errs = append(errs, errors.New("qq.app_secret is required"))
	} else if c.QQ.AppSecret != strings.TrimSpace(c.QQ.AppSecret) {
		errs = append(errs, errors.New("qq.app_secret must not have surrounding whitespace"))
	}
	return errors.Join(errs...)
}

func defaults() Config {
	return Config{
		Server: ServerConfig{
			Address:         "127.0.0.1",
			Port:            defaultPort,
			AlertPath:       defaultAlertPath,
			MaxBodyBytes:    defaultMaxBodyBytes,
			ShutdownTimeout: defaultShutdownTimeout.String(),
		},
		QQ: QQConfig{
			RequestTimeout:      defaultRequestTimeout.String(),
			DeliveryTimeout:     defaultDeliveryTimeout.String(),
			MaxPendingAlerts:    defaultMaxPendingAlerts,
			GatewayReadyTimeout: defaultGatewayTimeout.String(),
		},
		Message: MessageConfig{
			Prefix:    "[Gatus]",
			MaxLength: defaultMessageMaxLength,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "text",
		},
	}
}

func validateTarget(target TargetConfig) error {
	var errs []error
	switch target.Type {
	case "channel", "group", "user":
	default:
		errs = append(errs, errors.New("type must be channel, group, or user"))
	}
	if strings.TrimSpace(target.ID) == "" {
		errs = append(errs, errors.New("id is required"))
	} else if target.ID != strings.TrimSpace(target.ID) {
		errs = append(errs, errors.New("id must not have surrounding whitespace"))
	}
	if target.Name != strings.TrimSpace(target.Name) {
		errs = append(errs, errors.New("name must not have surrounding whitespace"))
	}
	return errors.Join(errs...)
}

func validateLog(cfg LogConfig) error {
	var errs []error
	switch strings.ToLower(cfg.Level) {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, errors.New("log.level must be debug, info, warn, or error"))
	}
	switch strings.ToLower(cfg.Format) {
	case "text", "json":
	default:
		errs = append(errs, errors.New("log.format must be text or json"))
	}
	return errors.Join(errs...)
}

func validHTTPPath(value string) bool {
	if value == "/" || !strings.HasPrefix(value, "/") || value != path.Clean(value) ||
		strings.ContainsAny(value, "?#{} \t\r\n") {
		return false
	}
	_, err := url.ParseRequestURI(value)
	return err == nil
}

func isLoopbackAddress(value string) bool {
	if strings.EqualFold(value, "localhost") {
		return true
	}
	ip := net.ParseIP(value)
	return ip != nil && ip.IsLoopback()
}

func positiveDuration(name, value string) (time.Duration, error) {
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", name, err)
	}
	if duration <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return duration, nil
}
