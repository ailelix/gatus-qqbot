package qqbot

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tencent-connect/botgo/dto"
	"github.com/tencent-connect/botgo/errs"
	"github.com/tencent-connect/botgo/event"
	"golang.org/x/oauth2"
)

// resumedEventType is the event QQ sends once a resume succeeds. BotGo has no
// parser for it, so it reaches the gateway through the plain event handler.
const resumedEventType = dto.EventType("RESUMED")

type gatewayAPIFunc func(context.Context, string, string, interface{}) ([]byte, error)

func (f gatewayAPIFunc) Transport(ctx context.Context, method, url string, body interface{}) ([]byte, error) {
	return f(ctx, method, url, body)
}

type sessionManagerFunc func(context.Context, *dto.WebsocketAP, oauth2.TokenSource, *dto.Intent, sessionObserver) error

func (f sessionManagerFunc) Start(
	ctx context.Context,
	apInfo *dto.WebsocketAP,
	source oauth2.TokenSource,
	intents *dto.Intent,
	observer sessionObserver,
) error {
	return f(ctx, apInfo, source, intents, observer)
}

func TestGatewayDebugLogsGroupOpenIDWithoutMessageContent(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	gateway := newGateway(nil, nil, nil, time.Second, logger)
	if intents := gateway.registerHandlers(); intents != dto.IntentGroupMessages {
		t.Fatalf("intents = %d, want %d", intents, dto.IntentGroupMessages)
	}
	identity := &dto.Session{}
	gateway.sessionStarted(identity, resumableSession{})
	payload := &dto.WSPayload{Session: identity, RawMessage: []byte(`{
		"d": {
			"group_openid": "group-openid-for-config",
			"content": "sensitive message body",
			"author": {"user_openid": "private-user-openid"}
		}
	}`)}
	if err := event.DefaultHandlers.GroupATMessage(payload, nil); err != nil {
		t.Fatalf("GroupATMessage() error = %v", err)
	}
	logOutput := output.String()
	if !strings.Contains(logOutput, "received QQ group message") ||
		!strings.Contains(logOutput, "group-openid-for-config") {
		t.Fatalf("group OpenID log is incomplete: %s", logOutput)
	}
	for _, secret := range []string{"sensitive message body", "private-user-openid"} {
		if strings.Contains(logOutput, secret) {
			t.Fatalf("group event log leaked %q: %s", secret, logOutput)
		}
	}
}

func TestGroupOpenIDFromEventSupportsLegacyPayload(t *testing.T) {
	data := &dto.WSGroupATMessageData{GroupID: "legacy-group-id"}
	if got := groupOpenIDFromEvent(&dto.WSPayload{RawMessage: []byte(`not JSON`)}, data); got != data.GroupID {
		t.Fatalf("groupOpenIDFromEvent() = %q, want %q", got, data.GroupID)
	}
}

func TestGatewayDiscoversModernEndpointAndWaitsForContext(t *testing.T) {
	source := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "token", TokenType: "QQBot"})
	type request struct {
		method string
		url    string
		body   interface{}
	}
	requestCh := make(chan request, 1)
	api := gatewayAPIFunc(func(_ context.Context, method, url string, body interface{}) ([]byte, error) {
		requestCh <- request{method: method, url: url, body: body}
		return []byte(`{"url":"wss://gateway.example.test/websocket"}`), nil
	})

	type startCall struct {
		apInfo  *dto.WebsocketAP
		source  oauth2.TokenSource
		intents dto.Intent
	}
	startCh := make(chan startCall, 1)
	manager := sessionManagerFunc(func(ctx context.Context, apInfo *dto.WebsocketAP, gotSource oauth2.TokenSource, intents *dto.Intent, observer sessionObserver) error {
		startCh <- startCall{apInfo: apInfo, source: gotSource, intents: *intents}
		identity := &dto.Session{}
		observer.sessionStarted(identity, resumableSession{Shards: dto.ShardConfig{ShardCount: 1}})
		event.DefaultHandlers.Ready(gatewayEvent(identity, 1, "READY"), &dto.WSReadyData{
			SessionID: "initial-session",
			Shard:     []uint32{0, 1},
		})
		<-ctx.Done()
		return nil
	})

	gateway := newGateway(api, source, manager, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gateway.Run(ctx) }()

	select {
	case <-gateway.Ready():
	case <-time.After(time.Second):
		t.Fatal("gateway did not report READY")
	}
	req := <-requestCh
	if req.method != http.MethodGet || req.url != gatewayEndpoint || req.body != nil {
		t.Fatalf("gateway request = %#v", req)
	}
	call := <-startCh
	if call.apInfo.URL != "wss://gateway.example.test/websocket" || call.apInfo.Shards != 1 ||
		call.apInfo.SessionStartLimit.Remaining != 1 || call.apInfo.SessionStartLimit.MaxConcurrency != 1 {
		t.Fatalf("session info = %#v", call.apInfo)
	}
	if call.source != source {
		t.Fatal("gateway did not reuse the client's token source")
	}
	if call.intents != dto.IntentGroupMessages {
		t.Fatalf("intents = %d, want %d", call.intents, dto.IntentGroupMessages)
	}
	if event.DefaultHandlers.C2CMessage == nil || event.DefaultHandlers.GroupATMessage == nil {
		t.Fatal("incoming QQ message handlers were not registered")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() after cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop after cancellation")
	}
}

func TestGatewayReportsDiscoveryAndManagerErrors(t *testing.T) {
	source := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "token"})
	manager := sessionManagerFunc(func(context.Context, *dto.WebsocketAP, oauth2.TokenSource, *dto.Intent, sessionObserver) error {
		return errors.New("session failed")
	})
	tests := []struct {
		name string
		api  gatewayAPIFunc
		want string
	}{
		{
			name: "request",
			api: func(context.Context, string, string, interface{}) ([]byte, error) {
				return nil, errors.New("request failed")
			},
			want: "discover QQ gateway: request failed",
		},
		{
			name: "JSON",
			api: func(context.Context, string, string, interface{}) ([]byte, error) {
				return []byte(`{`), nil
			},
			want: "decode QQ gateway response",
		},
		{
			name: "URL",
			api: func(context.Context, string, string, interface{}) ([]byte, error) {
				return []byte(`{"url":"https://example.test/not-websocket"}`), nil
			},
			want: "invalid WebSocket URL",
		},
		{
			name: "manager",
			api: func(context.Context, string, string, interface{}) ([]byte, error) {
				return []byte(`{"url":"wss://gateway.example.test/websocket"}`), nil
			},
			want: "run QQ gateway: session failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gateway := newGateway(tt.api, source, manager, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
			err := gateway.Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Run() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestGatewayAllowsRecoverableReconnects(t *testing.T) {
	tests := []struct {
		name    string
		recover func(t *testing.T, identity *dto.Session)
	}{
		{
			name: "resume",
			recover: func(t *testing.T, identity *dto.Session) {
				t.Helper()
				payload := gatewayEvent(identity, 2, resumedEventType)
				if err := event.DefaultHandlers.Plain(payload, payload.RawMessage); err != nil {
					t.Fatalf("Plain(RESUMED) error = %v", err)
				}
			},
		},
		{
			name: "re-identify",
			recover: func(t *testing.T, identity *dto.Session) {
				t.Helper()
				event.DefaultHandlers.Ready(gatewayEvent(identity, 2, "READY"), &dto.WSReadyData{
					SessionID: "replacement-session",
					Shard:     []uint32{0, 1},
				})
			},
		},
		{
			// Events replayed after a resume arrive before RESUMED, and QQ is
			// the only source of that event, so traffic alone must count.
			name: "replayed event without RESUMED",
			recover: func(t *testing.T, identity *dto.Session) {
				t.Helper()
				payload := gatewayEvent(identity, 2, dto.EventGroupAtMessageCreate)
				if err := event.DefaultHandlers.GroupATMessage(payload, nil); err != nil {
					t.Fatalf("GroupATMessage() error = %v", err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const recoveryTimeout = 40 * time.Millisecond
			disconnect := make(chan struct{})
			nextAttempt := make(chan *dto.Session, 1)
			manager := sessionManagerFunc(func(ctx context.Context, _ *dto.WebsocketAP, _ oauth2.TokenSource, _ *dto.Intent, observer sessionObserver) error {
				first := &dto.Session{}
				observer.sessionStarted(first, resumableSession{Shards: dto.ShardConfig{ShardCount: 1}})
				event.DefaultHandlers.Ready(gatewayEvent(first, 1, "READY"), &dto.WSReadyData{
					SessionID: "initial-session",
					Shard:     []uint32{0, 1},
				})
				<-disconnect
				resume := observer.sessionFailed(first, errors.New("recoverable disconnect"), false, false)
				second := &dto.Session{}
				observer.sessionStarted(second, resume)
				nextAttempt <- second
				<-ctx.Done()
				return nil
			})
			gateway := newGateway(successfulGatewayAPI(), nil, manager, recoveryTimeout, discardGatewayLogger())
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- gateway.Run(ctx) }()

			awaitGatewayReady(t, gateway)
			close(disconnect)
			identity := <-nextAttempt
			tt.recover(t, identity)
			select {
			case err := <-done:
				cancel()
				t.Fatalf("Run() stopped after a recovered connection: %v", err)
			case <-time.After(2 * recoveryTimeout):
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("Run() after cancellation error = %v", err)
			}
		})
	}
}

func TestGatewayRejectsStaleRecoveryAndFailsWhenReconnectTimesOut(t *testing.T) {
	want := errors.New("websocket read failed")
	disconnect := make(chan struct{})
	identities := make(chan *dto.Session, 2)
	manager := sessionManagerFunc(func(ctx context.Context, _ *dto.WebsocketAP, _ oauth2.TokenSource, _ *dto.Intent, observer sessionObserver) error {
		first := &dto.Session{}
		observer.sessionStarted(first, resumableSession{Shards: dto.ShardConfig{ShardCount: 1}})
		identities <- first
		event.DefaultHandlers.Ready(gatewayEvent(first, 1, "READY"), &dto.WSReadyData{
			SessionID: "initial-session",
			Shard:     []uint32{0, 1},
		})
		<-disconnect
		resume := observer.sessionFailed(first, want, false, false)
		second := &dto.Session{}
		observer.sessionStarted(second, resume)
		identities <- second
		<-ctx.Done()
		return nil
	})
	const recoveryTimeout = 20 * time.Millisecond
	gateway := newGateway(successfulGatewayAPI(), nil, manager, recoveryTimeout, discardGatewayLogger())
	done := make(chan error, 1)
	go func() { done <- gateway.Run(context.Background()) }()

	awaitGatewayReady(t, gateway)
	first := <-identities
	close(disconnect)
	<-identities
	stale := gatewayEvent(first, 2, resumedEventType)
	if err := event.DefaultHandlers.Plain(stale, stale.RawMessage); err != nil {
		t.Fatalf("Plain(stale RESUMED) error = %v", err)
	}
	event.DefaultHandlers.Ready(gatewayEvent(first, 3, "READY"), &dto.WSReadyData{
		SessionID: "stale-session",
		Shard:     []uint32{0, 1},
	})
	event.DefaultHandlers.ErrorNotify(errors.New("unattributed SDK callback"))
	if state := gateway.connectionState(); !state.recovering || state.resume.ID == "stale-session" {
		t.Fatalf("stale recovery changed state: %#v", state)
	}
	select {
	case err := <-done:
		if !errors.Is(err, want) || !strings.Contains(err.Error(), "did not recover") {
			t.Fatalf("Run() error = %v, want recovery timeout wrapping %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not report the reconnect timeout")
	}
}

func TestGatewayReportsFatalConnectionErrorsImmediately(t *testing.T) {
	want := errs.New(errs.CodeConnCloseCantIdentify, "bot is unavailable")
	disconnect := make(chan struct{})
	manager := sessionManagerFunc(func(ctx context.Context, _ *dto.WebsocketAP, _ oauth2.TokenSource, _ *dto.Intent, observer sessionObserver) error {
		identity := &dto.Session{}
		observer.sessionStarted(identity, resumableSession{Shards: dto.ShardConfig{ShardCount: 1}})
		event.DefaultHandlers.Ready(gatewayEvent(identity, 1, "READY"), &dto.WSReadyData{
			SessionID: "initial-session",
			Shard:     []uint32{0, 1},
		})
		<-disconnect
		observer.sessionFailed(identity, want, false, true)
		<-ctx.Done()
		return nil
	})
	gateway := newGateway(successfulGatewayAPI(), nil, manager, time.Minute, discardGatewayLogger())
	done := make(chan error, 1)
	go func() { done <- gateway.Run(context.Background()) }()

	awaitGatewayReady(t, gateway)
	close(disconnect)
	select {
	case err := <-done:
		if !errors.Is(err, want) || !strings.Contains(err.Error(), "cannot reconnect") {
			t.Fatalf("Run() error = %v, want fatal error wrapping %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not report the fatal connection error")
	}
}

func TestGatewayRecoveryDeadlineIsStableUntilRecovery(t *testing.T) {
	gateway := newGateway(nil, nil, nil, time.Second, discardGatewayLogger())
	first := &dto.Session{}
	gateway.sessionStarted(first, resumableSession{Shards: dto.ShardConfig{ShardCount: 1}})
	gateway.sessionFailed(first, errors.New("first failure"), false, false)
	firstFailure := gateway.connectionState()

	second := &dto.Session{}
	gateway.sessionStarted(second, firstFailure.resume)
	gateway.sessionFailed(second, errors.New("second failure"), false, false)
	secondFailure := gateway.connectionState()
	if secondFailure.recoveryGeneration != firstFailure.recoveryGeneration ||
		!secondFailure.disconnectedAt.Equal(firstFailure.disconnectedAt) {
		t.Fatalf("repeated failure reset recovery deadline: first=%#v second=%#v", firstFailure, secondFailure)
	}

	third := &dto.Session{}
	gateway.sessionStarted(third, secondFailure.resume)
	if !gateway.observeEvent(gatewayEvent(third, 3, resumedEventType)) {
		t.Fatal("observeEvent(RESUMED) rejected an event from the current session")
	}
	if gateway.connectionState().recovering {
		t.Fatal("observeEvent(RESUMED) did not end the recovery")
	}
	time.Sleep(time.Millisecond)
	gateway.sessionFailed(third, errors.New("third failure"), false, false)
	thirdFailure := gateway.connectionState()
	if thirdFailure.recoveryGeneration != firstFailure.recoveryGeneration+1 ||
		!thirdFailure.disconnectedAt.After(firstFailure.disconnectedAt) {
		t.Fatalf("new outage did not get a new deadline: first=%#v third=%#v", firstFailure, thirdFailure)
	}
}

func TestGatewayTracksEveryGroupIntentEventSequence(t *testing.T) {
	gateway := newGateway(nil, nil, nil, time.Second, discardGatewayLogger())
	if intents := gateway.registerHandlers(); intents != dto.IntentGroupMessages {
		t.Fatalf("intents = %d, want %d", intents, dto.IntentGroupMessages)
	}
	identity := &dto.Session{}
	gateway.sessionStarted(identity, resumableSession{Shards: dto.ShardConfig{ShardCount: 1}})

	if err := event.DefaultHandlers.GroupATMessage(gatewayEvent(identity, 2, dto.EventGroupAtMessageCreate), nil); err != nil {
		t.Fatalf("GroupATMessage() error = %v", err)
	}
	if err := event.DefaultHandlers.C2CMessage(gatewayEvent(identity, 3, dto.EventC2CMessageCreate), nil); err != nil {
		t.Fatalf("C2CMessage() error = %v", err)
	}
	if err := event.DefaultHandlers.SubscribeMsgStatus(gatewayEvent(identity, 4, dto.EventSubscribeMsgStatus), nil); err != nil {
		t.Fatalf("SubscribeMsgStatus() error = %v", err)
	}
	if err := event.DefaultHandlers.C2CFriend(gatewayEvent(identity, 5, dto.EventC2CFriendAdd), nil); err != nil {
		t.Fatalf("C2CFriend() error = %v", err)
	}
	plain := gatewayEvent(identity, 6, "UNRELATED")
	if err := event.DefaultHandlers.Plain(plain, nil); err != nil {
		t.Fatalf("Plain() error = %v", err)
	}

	resume := gateway.sessionFailed(identity, errors.New("disconnect"), false, false)
	if resume.LastSeq != 6 {
		t.Fatalf("resume LastSeq = %d, want 6", resume.LastSeq)
	}
}

func TestGatewayDoesNotDiscoverAfterCancellation(t *testing.T) {
	called := false
	api := gatewayAPIFunc(func(context.Context, string, string, interface{}) ([]byte, error) {
		called = true
		return nil, nil
	})
	gateway := newGateway(api, nil, nil, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := gateway.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if called {
		t.Fatal("Gateway API was called after cancellation")
	}
}

func successfulGatewayAPI() gatewayAPIFunc {
	return func(context.Context, string, string, interface{}) ([]byte, error) {
		return []byte(`{"url":"wss://gateway.example.test/websocket"}`), nil
	}
}

func discardGatewayLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func awaitGatewayReady(t *testing.T, gateway *Gateway) {
	t.Helper()
	select {
	case <-gateway.Ready():
	case <-time.After(time.Second):
		t.Fatal("gateway did not report READY")
	}
}

func gatewayEvent(identity *dto.Session, seq uint32, eventType dto.EventType) *dto.WSPayload {
	return &dto.WSPayload{
		WSPayloadBase: dto.WSPayloadBase{
			OPCode: dto.WSDispatchEvent,
			Seq:    seq,
			Type:   eventType,
		},
		Session: identity,
	}
}
