package qqbot

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tencent-connect/botgo/dto"
	"github.com/tencent-connect/botgo/sessions/manager"
	"github.com/tencent-connect/botgo/websocket"
	"golang.org/x/oauth2"
)

type resumableSession struct {
	ID      string
	LastSeq uint32
	Shards  dto.ShardConfig
}

type sessionObserver interface {
	sessionStarted(*dto.Session, resumableSession)
	sessionFailed(*dto.Session, error, bool, bool) resumableSession
}

type sessionManager interface {
	Start(context.Context, *dto.WebsocketAP, oauth2.TokenSource, *dto.Intent, sessionObserver) error
}

type localGatewaySessionManager struct {
	factory       websocket.WebSocket
	retryInterval func(uint32) time.Duration
}

const websocketListenerShutdownTimeout = time.Second

func newLocalGatewaySessionManager(factory websocket.WebSocket) *localGatewaySessionManager {
	return &localGatewaySessionManager{
		factory:       factory,
		retryInterval: manager.CalcInterval,
	}
}

// Start supervises the single shard used by this service. BotGo still owns the
// WebSocket protocol; this loop owns retries so failed Identify/Resume calls
// cannot silently remove the only session.
func (m *localGatewaySessionManager) Start(
	ctx context.Context,
	apInfo *dto.WebsocketAP,
	tokenSource oauth2.TokenSource,
	intents *dto.Intent,
	observer sessionObserver,
) error {
	if err := ctx.Err(); err != nil {
		return nil
	}
	if apInfo == nil || intents == nil || observer == nil {
		return errors.New("start QQ gateway session: invalid arguments")
	}
	if m.factory == nil {
		return errors.New("start QQ gateway session: BotGo WebSocket client is not registered")
	}
	if err := manager.CheckSessionLimit(apInfo); err != nil {
		return fmt.Errorf("start QQ gateway session: %w", err)
	}
	if apInfo.Shards != 1 {
		return fmt.Errorf("start QQ gateway session: got %d shards, want 1", apInfo.Shards)
	}

	retryInterval := manager.CalcInterval(apInfo.SessionStartLimit.MaxConcurrency)
	if m.retryInterval != nil {
		retryInterval = m.retryInterval(apInfo.SessionStartLimit.MaxConcurrency)
	}
	resume := resumableSession{Shards: dto.ShardConfig{ShardCount: 1}}

	for attempt := 0; ; attempt++ {
		// The start interval paces reconnects against QQ's session start limit.
		// The first attempt has nothing to pace against, and the HTTP listener
		// stays down until this session reaches READY.
		delay := retryInterval
		if attempt == 0 {
			delay = 0
		}
		if !waitForSessionAttempt(ctx, delay) {
			return nil
		}
		session := dto.Session{
			ID:          resume.ID,
			URL:         apInfo.URL,
			TokenSource: tokenSource,
			Intent:      *intents,
			LastSeq:     resume.LastSeq,
			Shards:      resume.Shards,
		}
		client, identity, err := newWebSocketAttempt(m.factory, session)
		if err != nil {
			return err
		}
		observer.sessionStarted(identity, resume)

		err = runWebSocketAttempt(ctx, client, resume.ID != "")
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			err = errors.New("BotGo WebSocket listener stopped unexpectedly")
		}
		var fatal bool
		resume, fatal = closeSessionAttempt(observer, identity, err)
		if fatal {
			return err
		}
	}
}

func newWebSocketAttempt(
	factory websocket.WebSocket,
	session dto.Session,
) (client websocket.WebSocket, identity *dto.Session, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			client = nil
			identity = nil
			err = fmt.Errorf("create BotGo WebSocket client: panic: %v", recovered)
		}
	}()
	client = factory.New(session)
	if client == nil {
		return nil, nil, errors.New("start QQ gateway session: BotGo returned an invalid WebSocket client")
	}
	identity = client.Session()
	if identity == nil {
		return nil, nil, errors.New("start QQ gateway session: BotGo returned an invalid WebSocket client")
	}
	return client, identity, nil
}

func runWebSocketAttempt(ctx context.Context, client websocket.WebSocket, resume bool) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			safeCloseWebSocket(client)
			err = fmt.Errorf("run BotGo WebSocket client: panic: %v", recovered)
		}
	}()

	if err = client.Connect(); err != nil {
		return err
	}
	if ctx.Err() != nil {
		safeCloseWebSocket(client)
		return ctx.Err()
	}
	if resume {
		err = client.Resume()
	} else {
		err = client.Identify()
	}
	if err != nil {
		safeCloseWebSocket(client)
		return err
	}
	if ctx.Err() != nil {
		safeCloseWebSocket(client)
		return ctx.Err()
	}

	listening := make(chan error, 1)
	go func() {
		listening <- listenWebSocket(client)
	}()
	select {
	case <-ctx.Done():
		safeCloseWebSocket(client)
		waitForWebSocketListener(listening, websocketListenerShutdownTimeout)
		return ctx.Err()
	case err = <-listening:
		return err
	}
}

func waitForWebSocketListener(listening <-chan error, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-listening:
	case <-timer.C:
	}
}

func listenWebSocket(client websocket.WebSocket) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("listen with BotGo WebSocket client: panic: %v", recovered)
		}
	}()
	return client.Listening()
}

func safeCloseWebSocket(client websocket.WebSocket) {
	defer func() { _ = recover() }()
	client.Close()
}

func closeSessionAttempt(
	observer sessionObserver,
	identity *dto.Session,
	err error,
) (resumableSession, bool) {
	cannotResume := manager.CanNotResume(err)
	cannotIdentify := manager.CanNotIdentify(err)
	return observer.sessionFailed(identity, err, cannotResume, cannotIdentify), cannotIdentify
}

func waitForSessionAttempt(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
