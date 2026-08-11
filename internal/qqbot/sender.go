package qqbot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ailelix/gatus-qqbot/internal/config"
	"github.com/ailelix/gatus-qqbot/internal/delivery"
	"github.com/tencent-connect/botgo/dto"
	"github.com/tencent-connect/botgo/openapi/options"
)

type messageAPI interface {
	PostMessage(context.Context, string, *dto.MessageToCreate, ...options.Option) (*dto.Message, error)
	PostGroupMessage(context.Context, string, dto.APIMessage, ...options.Option) (*dto.Message, error)
	PostC2CMessage(context.Context, string, dto.APIMessage, ...options.Option) (*dto.Message, error)
}

type Sender struct {
	api       messageAPI
	targets   []config.TargetConfig
	logger    *slog.Logger
	gate      chan struct{}
	admission chan struct{}
}

func NewSender(api messageAPI, targets []config.TargetConfig, maxPendingAlerts int, logger *slog.Logger) *Sender {
	if maxPendingAlerts < 0 {
		maxPendingAlerts = 0
	}
	return &Sender{
		api:       api,
		targets:   append([]config.TargetConfig(nil), targets...),
		logger:    logger,
		gate:      make(chan struct{}, 1),
		admission: make(chan struct{}, maxPendingAlerts+1),
	}
}

// Send attempts every configured destination and returns all delivery errors.
func (s *Sender) Send(ctx context.Context, content string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.admission <- struct{}{}:
		defer func() { <-s.admission }()
	case <-ctx.Done():
		return ctx.Err()
	default:
		return delivery.ErrQueueFull
	}
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var errs []error
	for _, target := range s.targets {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		message := &dto.MessageToCreate{Content: content, MsgType: dto.TextMsg}
		var err error
		switch target.Type {
		case "channel":
			_, err = s.api.PostMessage(ctx, target.ID, message)
		case "group":
			_, err = s.api.PostGroupMessage(ctx, target.ID, message)
		case "user":
			_, err = s.api.PostC2CMessage(ctx, target.ID, message)
		default:
			err = fmt.Errorf("unsupported target type %q", target.Type)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("deliver to %s: %w", targetLabel(target), err))
			if ctxErr := ctx.Err(); ctxErr != nil {
				errs = append(errs, ctxErr)
				break
			}
			continue
		}
		if s.logger != nil {
			s.logger.Debug("delivered QQ message", "target", targetLabel(target), "type", target.Type)
		}
	}
	return errors.Join(errs...)
}

func targetLabel(target config.TargetConfig) string {
	if target.Name != "" {
		return target.Name
	}
	return target.Type + ":" + target.ID
}
