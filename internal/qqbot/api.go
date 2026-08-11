package qqbot

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tencent-connect/botgo"
	"github.com/tencent-connect/botgo/constant"
	"github.com/tencent-connect/botgo/openapi"
	"github.com/tencent-connect/botgo/token"
	"golang.org/x/oauth2"
)

const currentAPIDomain = "https://api.bot.qq.com"

var configureSDK sync.Once

// Client keeps the OpenAPI client and its token source together so REST calls
// and the Gateway session share the same cached access token.
type Client struct {
	openapi.OpenAPI
	tokenSource oauth2.TokenSource
}

func NewClient(ctx context.Context, appID, appSecret string, timeout time.Duration) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	configureSDK.Do(func() {
		// BotGo v0.2.1 predates QQ's 2026 API-domain consolidation.
		constant.APIDomain = currentAPIDomain
		constant.TokenDomain = currentAPIDomain
		// The empty adapter reads slog.Default dynamically. CLI configures the
		// default before constructing a client.
		botgo.SetLogger(SlogAdapter{})
	})
	tokenSource := token.NewQQBotTokenSource(&token.QQBotCredentials{
		AppID:     appID,
		AppSecret: appSecret,
	})
	if _, err := tokenSource.Token(); err != nil {
		return nil, fmt.Errorf("authenticate QQ bot: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &Client{
		OpenAPI:     botgo.NewOpenAPI(appID, tokenSource).WithTimeout(timeout),
		tokenSource: tokenSource,
	}, nil
}
