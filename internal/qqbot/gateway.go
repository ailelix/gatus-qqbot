package qqbot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/tencent-connect/botgo/dto"
	"github.com/tencent-connect/botgo/event"
	"github.com/tencent-connect/botgo/websocket"
	"golang.org/x/oauth2"
)

const (
	gatewayEndpoint               = currentAPIDomain + "/gateway"
	defaultGatewayRecoveryTimeout = 30 * time.Second
	gatewayManagerShutdownTimeout = 2 * time.Second
)

type gatewayAPI interface {
	Transport(context.Context, string, string, interface{}) ([]byte, error)
}

// Gateway maintains the event connection QQ uses to determine whether the bot
// service is online. Incoming messages are deliberately ignored.
type Gateway struct {
	api             gatewayAPI
	tokenSource     oauth2.TokenSource
	manager         sessionManager
	logger          *slog.Logger
	recoveryTimeout time.Duration
	ready           chan struct{}
	readyOnce       sync.Once
	stateChanged    chan struct{}
	stateMu         sync.Mutex
	state           gatewayConnectionState
}

type gatewayConnectionState struct {
	currentSession     *dto.Session
	currentSessionOpen bool
	resume             resumableSession
	recovering         bool
	recoveryGeneration uint64
	disconnectedAt     time.Time
	lastError          error
	fatalError         error
}

func NewGateway(client *Client, recoveryTimeout time.Duration, logger *slog.Logger) *Gateway {
	return newGateway(
		client,
		client.tokenSource,
		newLocalGatewaySessionManager(websocket.ClientImpl),
		recoveryTimeout,
		logger,
	)
}

func newGateway(
	api gatewayAPI,
	tokenSource oauth2.TokenSource,
	manager sessionManager,
	recoveryTimeout time.Duration,
	logger *slog.Logger,
) *Gateway {
	if logger == nil {
		logger = slog.Default()
	}
	if recoveryTimeout <= 0 {
		recoveryTimeout = defaultGatewayRecoveryTimeout
	}
	return &Gateway{
		api:             api,
		tokenSource:     tokenSource,
		manager:         manager,
		logger:          logger,
		recoveryTimeout: recoveryTimeout,
		ready:           make(chan struct{}),
		stateChanged:    make(chan struct{}, 1),
	}
}

func (g *Gateway) Ready() <-chan struct{} {
	return g.ready
}

func (g *Gateway) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	g.logger.Info("connecting to QQ gateway")
	apInfo, err := g.discover(ctx)
	if err != nil {
		return err
	}

	intents := g.registerHandlers()
	if intents != dto.IntentGroupMessages {
		return fmt.Errorf("configure QQ gateway intents: got %d, want %d", intents, dto.IntentGroupMessages)
	}

	managerCtx, cancelManager := context.WithCancel(ctx)
	defer cancelManager()
	errCh := make(chan error, 1)
	go func() {
		errCh <- g.manager.Start(managerCtx, apInfo, g.tokenSource, &intents, g)
	}()

	var recoveryTimer *time.Timer
	var recoveryTimerC <-chan time.Time
	var recoveryGeneration uint64
	defer func() { stopTimer(recoveryTimer) }()

	for {
		select {
		case <-ctx.Done():
			g.stopSessionManager(cancelManager, errCh)
			return nil
		case err := <-errCh:
			if ctx.Err() != nil {
				return nil
			}
			if state := g.connectionState(); state.fatalError != nil {
				return fmt.Errorf("QQ gateway cannot reconnect: %w", state.fatalError)
			}
			if err == nil {
				return errors.New("QQ gateway session manager stopped unexpectedly")
			}
			return fmt.Errorf("run QQ gateway: %w", err)
		case <-g.stateChanged:
			if ctx.Err() != nil {
				g.stopSessionManager(cancelManager, errCh)
				return nil
			}
			state := g.connectionState()
			if state.fatalError != nil {
				cancelManager()
				return fmt.Errorf("QQ gateway cannot reconnect: %w", state.fatalError)
			}
			if !state.recovering {
				stopTimer(recoveryTimer)
				recoveryTimer = nil
				recoveryTimerC = nil
				continue
			}
			if recoveryTimer == nil || recoveryGeneration != state.recoveryGeneration {
				stopTimer(recoveryTimer)
				recoveryTimer = time.NewTimer(recoveryTimeRemaining(g.recoveryTimeout, state.disconnectedAt))
				recoveryTimerC = recoveryTimer.C
				recoveryGeneration = state.recoveryGeneration
			}
		case <-recoveryTimerC:
			recoveryTimer = nil
			recoveryTimerC = nil
			if ctx.Err() != nil {
				g.stopSessionManager(cancelManager, errCh)
				return nil
			}
			state := g.connectionState()
			if state.fatalError != nil {
				cancelManager()
				return fmt.Errorf("QQ gateway cannot reconnect: %w", state.fatalError)
			}
			if !state.recovering {
				continue
			}
			if recoveryGeneration != state.recoveryGeneration {
				recoveryTimer = time.NewTimer(recoveryTimeRemaining(g.recoveryTimeout, state.disconnectedAt))
				recoveryTimerC = recoveryTimer.C
				recoveryGeneration = state.recoveryGeneration
				continue
			}
			cancelManager()
			return fmt.Errorf("QQ gateway did not recover within %s: %w", g.recoveryTimeout, state.lastError)
		}
	}
}

func (g *Gateway) stopSessionManager(cancel context.CancelFunc, errCh <-chan error) {
	cancel()
	// BotGo's Connect and Write methods do not accept a context, so process
	// exit remains the final cleanup if cancellation lands while either call is
	// blocked past this bounded wait.
	timer := time.NewTimer(gatewayManagerShutdownTimeout)
	defer timer.Stop()
	select {
	case <-errCh:
	case <-timer.C:
		g.logger.Warn(
			"QQ gateway supervisor did not stop before the shutdown deadline",
			"timeout", gatewayManagerShutdownTimeout,
		)
	}
}

func (g *Gateway) registerHandlers() dto.Intent {
	return event.RegisterHandlers(
		event.ReadyHandler(func(payload *dto.WSPayload, data *dto.WSReadyData) {
			accepted, recovered := g.markReady(payload, data)
			if !accepted {
				g.logger.Debug("ignored READY from a closed QQ gateway session")
				return
			}
			firstReady := false
			g.readyOnce.Do(func() {
				firstReady = true
				close(g.ready)
			})
			if firstReady {
				botName := ""
				if data != nil {
					botName = data.User.Username
				}
				g.logger.Info("QQ gateway ready", "bot", botName)
			} else if recovered {
				g.logger.Info("QQ gateway recovered", "event", "READY")
			}
		}),
		event.ErrorNotifyHandler(func(err error) {
			if err == nil {
				err = errors.New("unknown connection error")
			}
			// Listening returns the same error to the session supervisor, which
			// can associate it with the exact connection attempt.
			g.logger.Debug("QQ gateway WebSocket reported a connection error", "error", err)
		}),
		// BotGo has no parser for RESUMED, so the event QQ sends after a
		// successful resume arrives here alongside any other unhandled event.
		event.PlainEventHandler(func(payload *dto.WSPayload, _ []byte) error {
			g.observeEvent(payload)
			return nil
		}),
		event.C2CMessageEventHandler(func(payload *dto.WSPayload, _ *dto.WSC2CMessageData) error {
			g.observeEvent(payload)
			return nil
		}),
		event.GroupATMessageEventHandler(func(payload *dto.WSPayload, data *dto.WSGroupATMessageData) error {
			if !g.observeEvent(payload) {
				return nil
			}
			if groupOpenID := groupOpenIDFromEvent(payload, data); groupOpenID != "" {
				g.logger.Debug("received QQ group message", "group_openid", groupOpenID)
			}
			return nil
		}),
		event.SubscribeMsgStatusEventHandler(func(payload *dto.WSPayload, _ *dto.WSSubscribeMsgStatus) error {
			g.observeEvent(payload)
			return nil
		}),
		event.C2CFriendEventHandler(func(payload *dto.WSPayload, _ *dto.WSC2CFriendData) error {
			g.observeEvent(payload)
			return nil
		}),
	)
}

func (g *Gateway) sessionStarted(identity *dto.Session, resume resumableSession) {
	g.stateMu.Lock()
	if g.state.fatalError == nil {
		g.state.currentSession = identity
		g.state.currentSessionOpen = true
		g.state.resume = resume
	}
	g.stateMu.Unlock()
}

func (g *Gateway) sessionFailed(
	identity *dto.Session,
	err error,
	cannotResume bool,
	cannotIdentify bool,
) resumableSession {
	g.stateMu.Lock()
	if identity != g.state.currentSession || !g.state.currentSessionOpen {
		resume := g.state.resume
		g.stateMu.Unlock()
		return resume
	}
	g.state.currentSessionOpen = false
	if cannotResume {
		g.state.resume.ID = ""
		g.state.resume.LastSeq = 0
	}
	if !g.state.recovering {
		g.state.recovering = true
		g.state.recoveryGeneration++
		g.state.disconnectedAt = time.Now()
	}
	g.state.lastError = err
	if cannotIdentify && g.state.fatalError == nil {
		g.state.fatalError = err
	}
	resume := g.state.resume
	g.stateMu.Unlock()
	g.notifyStateChanged()
	if cannotIdentify {
		g.logger.Error("QQ gateway cannot reconnect", "error", err)
	} else {
		g.logger.Warn("QQ gateway connection interrupted; retrying", "error", err)
	}
	return resume
}

func (g *Gateway) markReady(payload *dto.WSPayload, data *dto.WSReadyData) (bool, bool) {
	g.stateMu.Lock()
	if !g.acceptEventLocked(payload) {
		g.stateMu.Unlock()
		return false, false
	}
	g.recordSequenceLocked(payload)
	if data != nil {
		g.state.resume.ID = data.SessionID
		if len(data.Shard) >= 2 {
			g.state.resume.Shards = dto.ShardConfig{ShardID: data.Shard[0], ShardCount: data.Shard[1]}
		}
	}
	recovered := g.state.recovering
	g.state.recovering = false
	g.state.lastError = nil
	g.stateMu.Unlock()
	g.notifyStateChanged()
	return true, recovered
}

// observeEvent records inbound traffic for the active session and reports
// whether the payload belongs to it. Any accepted event ends a pending
// recovery: it can only have been read from the session opened after the
// disconnect, so it proves that session is live. Waiting for RESUMED alone
// would be fragile, because this service subscribes to group messages only and
// a quiet bot may never receive another event.
func (g *Gateway) observeEvent(payload *dto.WSPayload) bool {
	g.stateMu.Lock()
	if !g.acceptEventLocked(payload) {
		g.stateMu.Unlock()
		return false
	}
	g.recordSequenceLocked(payload)
	recovered := g.state.recovering
	g.state.recovering = false
	g.state.lastError = nil
	g.stateMu.Unlock()
	if recovered {
		g.notifyStateChanged()
		g.logger.Info("QQ gateway recovered", "event", string(payload.Type))
	}
	return true
}

func (g *Gateway) acceptEventLocked(payload *dto.WSPayload) bool {
	return payload != nil &&
		payload.Session != nil &&
		payload.Session == g.state.currentSession &&
		g.state.currentSessionOpen &&
		g.state.fatalError == nil
}

func (g *Gateway) recordSequenceLocked(payload *dto.WSPayload) {
	if payload.Seq > 0 {
		g.state.resume.LastSeq = payload.Seq
	}
}

func (g *Gateway) connectionState() gatewayConnectionState {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	return g.state
}

func (g *Gateway) notifyStateChanged() {
	select {
	case g.stateChanged <- struct{}{}:
	default:
	}
}

func recoveryTimeRemaining(timeout time.Duration, disconnectedAt time.Time) time.Duration {
	remaining := timeout - time.Since(disconnectedAt)
	if remaining <= 0 {
		return time.Nanosecond
	}
	return remaining
}

func stopTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func groupOpenIDFromEvent(payload *dto.WSPayload, data *dto.WSGroupATMessageData) string {
	if payload != nil && len(payload.RawMessage) != 0 {
		var envelope struct {
			Data struct {
				GroupOpenID string `json:"group_openid"`
				GroupID     string `json:"group_id"`
			} `json:"d"`
		}
		if err := json.Unmarshal(payload.RawMessage, &envelope); err == nil {
			if envelope.Data.GroupOpenID != "" {
				return envelope.Data.GroupOpenID
			}
			if envelope.Data.GroupID != "" {
				return envelope.Data.GroupID
			}
		}
	}
	if data != nil {
		return data.GroupID
	}
	return ""
}

func (g *Gateway) discover(ctx context.Context) (*dto.WebsocketAP, error) {
	body, err := g.api.Transport(ctx, http.MethodGet, gatewayEndpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("discover QQ gateway: %w", err)
	}
	var response struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("decode QQ gateway response: %w", err)
	}
	parsed, err := url.Parse(response.URL)
	if err != nil || parsed.Scheme != "wss" || parsed.Host == "" || parsed.User != nil {
		return nil, fmt.Errorf("QQ gateway returned an invalid WebSocket URL")
	}
	return &dto.WebsocketAP{
		URL:    response.URL,
		Shards: 1,
		SessionStartLimit: dto.SessionStartLimit{
			Total:          1,
			Remaining:      1,
			MaxConcurrency: 1,
		},
	}, nil
}
