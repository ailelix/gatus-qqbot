package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/ailelix/gatus-qqbot/internal/config"
	"github.com/ailelix/gatus-qqbot/internal/logging"
	"github.com/ailelix/gatus-qqbot/internal/qqbot"
	"github.com/ailelix/gatus-qqbot/internal/server"
	"github.com/mdp/qrterminal/v3"
	"github.com/spf13/cobra"
)

var Version = "dev"

func New(stdout, stderr io.Writer) *cobra.Command {
	var configPath string
	root := &cobra.Command{
		Use:           "gatus-qqbot",
		Short:         "Forward Gatus custom alerts to QQ",
		Version:       Version,
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	root.SetOut(stdout)
	root.SetErr(stderr)
	root.PersistentFlags().StringVarP(&configPath, "config", "c", "config.toml", "path to the TOML configuration")
	root.AddCommand(newServeCommand(&configPath, stderr))
	root.AddCommand(newAuthCommand(&configPath, stdout, stderr))
	return root
}

func newServeCommand(configPath *string, stderr io.Writer) *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Receive Gatus alerts and forward them to QQ",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(*configPath)
			if err != nil {
				return err
			}
			if err := cfg.ValidateServe(); err != nil {
				return fmt.Errorf("validate config: %w", err)
			}
			logger, err := logging.New(cfg.Log, stderr)
			if err != nil {
				return err
			}
			slog.SetDefault(logger)
			requestTimeout, _ := cfg.RequestTimeout()
			client, err := qqbot.NewClient(cmd.Context(), cfg.QQ.AppID, cfg.QQ.AppSecret, requestTimeout)
			if err != nil {
				return err
			}
			forwarder := qqbot.NewSender(client, cfg.QQ.Targets, cfg.QQ.MaxPendingAlerts, logger)
			shutdownTimeout, _ := cfg.ShutdownTimeout()
			deliveryTimeout, _ := cfg.DeliveryTimeout()
			gatewayReadyTimeout, _ := cfg.GatewayReadyTimeout()
			gateway := qqbot.NewGateway(client, gatewayReadyTimeout, logger)
			return runServeServices(cmd.Context(), gateway, gatewayReadyTimeout, func(ctx context.Context) error {
				deliveryCtx, cancelDeliveries := newDeliveryContext(ctx)
				defer cancelDeliveries()
				handler := server.NewHandler(server.HandlerOptions{
					AlertPath:       cfg.Server.AlertPath,
					AuthToken:       cfg.Server.AuthToken,
					MaxBodyBytes:    cfg.Server.MaxBodyBytes,
					MessagePrefix:   cfg.Message.Prefix,
					MessageMaxRunes: cfg.Message.MaxLength,
					DeliveryContext: deliveryCtx,
					DeliveryTimeout: deliveryTimeout,
					Logger:          logger,
				}, forwarder)
				if err := server.Run(ctx, cfg.ListenAddress(), handler, shutdownTimeout, logger); err != nil {
					return fmt.Errorf("run HTTP server: %w", err)
				}
				return nil
			})
		},
	}
}

func newDeliveryContext(serviceCtx context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(context.WithoutCancel(serviceCtx))
}

func newAuthCommand(configPath *string, stdout, stderr io.Writer) *cobra.Command {
	return &cobra.Command{
		Use:   "auth",
		Short: "Bind a QQ bot by scanning an Agent service QR code",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger := slog.New(slog.NewTextHandler(stderr, nil))
			slog.SetDefault(logger)
			connector := qqbot.NewQRConnector(&http.Client{Timeout: 10 * time.Second}, logger)
			qrCount := 0
			credentials, err := connector.Connect(cmd.Context(), "", func(url string) {
				if qrCount > 0 {
					_, _ = fmt.Fprintln(stdout, "The previous QR code expired; scan this refreshed code:")
				} else {
					_, _ = fmt.Fprintln(stdout, "Scan this QR code with mobile QQ to bind the bot:")
				}
				qrterminal.GenerateHalfBlock(url, qrterminal.L, stdout)
				_, _ = fmt.Fprintf(stdout, "URL: %s\n", url)
				qrCount++
			})
			if err != nil {
				return fmt.Errorf("bind QQ bot: %w", err)
			}
			return writeCredentials(stdout, *configPath, credentials)
		},
	}
}

func writeCredentials(w io.Writer, configPath string, credentials qqbot.QRCredentials) error {
	var snippet struct {
		QQ struct {
			AppID     string `toml:"app_id"`
			AppSecret string `toml:"app_secret"`
		} `toml:"qq"`
	}
	snippet.QQ.AppID = credentials.AppID
	snippet.QQ.AppSecret = credentials.AppSecret

	var encoded bytes.Buffer
	if err := toml.NewEncoder(&encoded).Encode(snippet); err != nil {
		return fmt.Errorf("encode QQ credentials: %w", err)
	}
	if _, err := fmt.Fprintf(w, "QQ bot binding completed. The file was not modified.\nStore these credentials in %q:\n\n%s", configPath, encoded.String()); err != nil {
		return fmt.Errorf("write QQ credentials: %w", err)
	}
	if credentials.UserOpenID != "" {
		if _, err := fmt.Fprintf(w, "\nScanner user_openid: %s\n", credentials.UserOpenID); err != nil {
			return fmt.Errorf("write QQ user OpenID: %w", err)
		}
	}
	return nil
}
