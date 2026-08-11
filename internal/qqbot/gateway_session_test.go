package qqbot

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tencent-connect/botgo/dto"
	"github.com/tencent-connect/botgo/errs"
	"github.com/tencent-connect/botgo/event"
	"github.com/tencent-connect/botgo/websocket"
)

func TestLocalGatewaySessionManagerConnectsWithoutInitialDelay(t *testing.T) {
	script := newWebSocketScript()
	factory := newScriptedWebSocketFactory(script)
	manager := newLocalGatewaySessionManager(factory)
	manager.retryInterval = func(uint32) time.Duration { return time.Minute }
	gateway := newGateway(nil, nil, nil, time.Second, discardGatewayLogger())
	gateway.registerHandlers()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- manager.Start(ctx, testWebsocketAP(), nil, testGatewayIntent(), gateway) }()

	select {
	case <-factory.created:
	case <-time.After(time.Second):
		t.Fatal("Start() waited for the retry interval before the first attempt")
	}
	awaitScriptListening(t, script)

	cancel()
	if err := awaitSessionManagerResult(t, done); err != nil {
		t.Fatalf("Start() after cancellation error = %v", err)
	}
}

func TestLocalGatewaySessionManagerRetriesResumeFailures(t *testing.T) {
	readErr := errors.New("websocket read failed")
	resumeErr := errors.New("resume write failed")
	first := newWebSocketScript()
	first.listenErr = readErr
	first.onListen = func(identity *dto.Session) {
		event.DefaultHandlers.Ready(gatewayEvent(identity, 7, "READY"), &dto.WSReadyData{
			SessionID: "resume-session",
			Shard:     []uint32{0, 1},
		})
	}
	second := newWebSocketScript()
	second.resumeErr = resumeErr
	third := newWebSocketScript()
	factory := newScriptedWebSocketFactory(first, second, third)
	manager := testLocalGatewaySessionManager(factory)
	gateway := newGateway(nil, nil, nil, time.Second, discardGatewayLogger())
	gateway.registerHandlers()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Start(ctx, testWebsocketAP(), nil, testGatewayIntent(), gateway) }()

	created := receiveCreatedSessions(t, factory.created, 3)
	awaitScriptListening(t, third)
	if created[0].ID != "" || created[0].LastSeq != 0 {
		t.Fatalf("initial session = %#v, want a fresh session", created[0])
	}
	for i, session := range created[1:] {
		if session.ID != "resume-session" || session.LastSeq != 7 {
			t.Fatalf("resume session %d = %#v, want ID and seq from READY", i+2, session)
		}
	}
	if !first.called("Identify") || first.called("Resume") {
		t.Fatalf("first attempt calls = %v, want Identify only", first.callsSnapshot())
	}
	for i, script := range []*webSocketScript{second, third} {
		if !script.called("Resume") || script.called("Identify") {
			t.Fatalf("resume attempt %d calls = %v, want Resume only", i+2, script.callsSnapshot())
		}
	}

	cancel()
	if err := awaitSessionManagerResult(t, done); err != nil {
		t.Fatalf("Start() after cancellation error = %v", err)
	}
	awaitScriptExit(t, third)
}

func TestLocalGatewaySessionManagerClearsInvalidSessionBeforeIdentify(t *testing.T) {
	first := newWebSocketScript()
	first.listenErr = errs.ErrInvalidSession
	first.onListen = func(identity *dto.Session) {
		event.DefaultHandlers.Ready(gatewayEvent(identity, 11, "READY"), &dto.WSReadyData{
			SessionID: "invalid-session",
			Shard:     []uint32{0, 1},
		})
	}
	second := newWebSocketScript()
	factory := newScriptedWebSocketFactory(first, second)
	manager := testLocalGatewaySessionManager(factory)
	gateway := newGateway(nil, nil, nil, time.Second, discardGatewayLogger())
	gateway.registerHandlers()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Start(ctx, testWebsocketAP(), nil, testGatewayIntent(), gateway) }()

	created := receiveCreatedSessions(t, factory.created, 2)
	awaitScriptListening(t, second)
	if created[1].ID != "" || created[1].LastSeq != 0 {
		t.Fatalf("session after invalidation = %#v, want cleared ID and seq", created[1])
	}
	if !second.called("Identify") || second.called("Resume") {
		t.Fatalf("second attempt calls = %v, want Identify only", second.callsSnapshot())
	}

	cancel()
	if err := awaitSessionManagerResult(t, done); err != nil {
		t.Fatalf("Start() after cancellation error = %v", err)
	}
}

func TestLocalGatewaySessionManagerRetriesConnectFailures(t *testing.T) {
	first := newWebSocketScript()
	first.connectErr = errors.New("dial failed")
	second := newWebSocketScript()
	factory := newScriptedWebSocketFactory(first, second)
	manager := testLocalGatewaySessionManager(factory)
	gateway := newGateway(nil, nil, nil, time.Second, discardGatewayLogger())
	gateway.registerHandlers()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Start(ctx, testWebsocketAP(), nil, testGatewayIntent(), gateway) }()

	receiveCreatedSessions(t, factory.created, 2)
	awaitScriptListening(t, second)
	if !second.called("Identify") {
		t.Fatalf("second attempt calls = %v, want Identify", second.callsSnapshot())
	}
	cancel()
	if err := awaitSessionManagerResult(t, done); err != nil {
		t.Fatalf("Start() after cancellation error = %v", err)
	}
}

func TestLocalGatewaySessionManagerRetriesIdentifyFailures(t *testing.T) {
	first := newWebSocketScript()
	first.identifyErr = errors.New("identify failed")
	second := newWebSocketScript()
	factory := newScriptedWebSocketFactory(first, second)
	manager := testLocalGatewaySessionManager(factory)
	gateway := newGateway(nil, nil, nil, time.Second, discardGatewayLogger())
	gateway.registerHandlers()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Start(ctx, testWebsocketAP(), nil, testGatewayIntent(), gateway) }()

	created := receiveCreatedSessions(t, factory.created, 2)
	awaitScriptListening(t, second)
	if created[1].ID != "" || created[1].LastSeq != 0 {
		t.Fatalf("session after Identify failure = %#v, want a fresh session", created[1])
	}
	if !second.called("Identify") || second.called("Resume") {
		t.Fatalf("second attempt calls = %v, want Identify only", second.callsSnapshot())
	}
	cancel()
	if err := awaitSessionManagerResult(t, done); err != nil {
		t.Fatalf("Start() after cancellation error = %v", err)
	}
}

func TestLocalGatewaySessionManagerRetriesRecoveredSDKPanics(t *testing.T) {
	first := newWebSocketScript()
	first.panicCall = "Identify"
	second := newWebSocketScript()
	factory := newScriptedWebSocketFactory(first, second)
	manager := testLocalGatewaySessionManager(factory)
	gateway := newGateway(nil, nil, nil, time.Second, discardGatewayLogger())
	gateway.registerHandlers()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Start(ctx, testWebsocketAP(), nil, testGatewayIntent(), gateway) }()

	receiveCreatedSessions(t, factory.created, 2)
	awaitScriptListening(t, second)
	if !first.called("Close") || !second.called("Identify") {
		t.Fatalf("attempt calls = first %v, second %v", first.callsSnapshot(), second.callsSnapshot())
	}
	cancel()
	if err := awaitSessionManagerResult(t, done); err != nil {
		t.Fatalf("Start() after cancellation error = %v", err)
	}
}

func TestLocalGatewaySessionManagerReturnsFatalErrors(t *testing.T) {
	want := errs.New(errs.CodeConnCloseCantIdentify, "bot unavailable")
	first := newWebSocketScript()
	first.connectErr = want
	factory := newScriptedWebSocketFactory(first)
	manager := testLocalGatewaySessionManager(factory)
	gateway := newGateway(nil, nil, nil, time.Second, discardGatewayLogger())
	gateway.registerHandlers()

	err := manager.Start(context.Background(), testWebsocketAP(), nil, testGatewayIntent(), gateway)
	if !errors.Is(err, want) {
		t.Fatalf("Start() error = %v, want %v", err, want)
	}
	if state := gateway.connectionState(); !errors.Is(state.fatalError, want) {
		t.Fatalf("fatal state = %v, want %v", state.fatalError, want)
	}
}

func TestRunWebSocketAttemptRecoversSDKPanics(t *testing.T) {
	for _, panicCall := range []string{"Connect", "Identify"} {
		t.Run(panicCall, func(t *testing.T) {
			script := newWebSocketScript()
			script.panicCall = panicCall
			factory := newScriptedWebSocketFactory(script)
			client := factory.New(dto.Session{})

			err := runWebSocketAttempt(context.Background(), client, false)
			if err == nil || !strings.Contains(err.Error(), "panic: "+panicCall+" panic") {
				t.Fatalf("runWebSocketAttempt() error = %v, want recovered panic", err)
			}
			if !script.called("Close") {
				t.Fatalf("calls = %v, want client close after panic", script.callsSnapshot())
			}
		})
	}
}

func TestWaitForSessionAttemptStopsDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- waitForSessionAttempt(ctx, time.Minute) }()
	cancel()
	select {
	case started := <-done:
		if started {
			t.Fatal("waitForSessionAttempt() started an attempt after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("waitForSessionAttempt() did not stop after cancellation")
	}
}

func testLocalGatewaySessionManager(factory websocket.WebSocket) *localGatewaySessionManager {
	manager := newLocalGatewaySessionManager(factory)
	manager.retryInterval = func(uint32) time.Duration { return 0 }
	return manager
}

func testWebsocketAP() *dto.WebsocketAP {
	return &dto.WebsocketAP{
		URL:    "wss://gateway.example.test/websocket",
		Shards: 1,
		SessionStartLimit: dto.SessionStartLimit{
			Remaining:      1,
			MaxConcurrency: 1,
		},
	}
}

func testGatewayIntent() *dto.Intent {
	intent := dto.IntentGroupMessages
	return &intent
}

type scriptedWebSocketFactory struct {
	mu      sync.Mutex
	scripts []*webSocketScript
	created chan dto.Session
}

func newScriptedWebSocketFactory(scripts ...*webSocketScript) *scriptedWebSocketFactory {
	return &scriptedWebSocketFactory{
		scripts: scripts,
		created: make(chan dto.Session, len(scripts)),
	}
}

func (f *scriptedWebSocketFactory) New(session dto.Session) websocket.WebSocket {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.scripts) == 0 {
		return nil
	}
	script := f.scripts[0]
	f.scripts = f.scripts[1:]
	identity := session
	script.identity = &identity
	f.created <- session
	return &scriptedWebSocketClient{script: script}
}

func (*scriptedWebSocketFactory) Connect() error        { return errors.New("factory cannot connect") }
func (*scriptedWebSocketFactory) Identify() error       { return errors.New("factory cannot identify") }
func (*scriptedWebSocketFactory) Session() *dto.Session { return nil }
func (*scriptedWebSocketFactory) Resume() error         { return errors.New("factory cannot resume") }
func (*scriptedWebSocketFactory) Listening() error      { return errors.New("factory cannot listen") }
func (*scriptedWebSocketFactory) Write(*dto.WSPayload) error {
	return errors.New("factory cannot write")
}
func (*scriptedWebSocketFactory) Close() {}

type webSocketScript struct {
	mu            sync.Mutex
	identity      *dto.Session
	calls         []string
	connectErr    error
	identifyErr   error
	resumeErr     error
	listenErr     error
	panicCall     string
	onListen      func(*dto.Session)
	listenStarted chan struct{}
	listenExited  chan struct{}
	closed        chan struct{}
	closeOnce     sync.Once
}

func newWebSocketScript() *webSocketScript {
	return &webSocketScript{
		listenStarted: make(chan struct{}),
		listenExited:  make(chan struct{}),
		closed:        make(chan struct{}),
	}
}

func (s *webSocketScript) record(call string) {
	s.mu.Lock()
	s.calls = append(s.calls, call)
	s.mu.Unlock()
}

func (s *webSocketScript) called(call string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, got := range s.calls {
		if got == call {
			return true
		}
	}
	return false
}

func (s *webSocketScript) callsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

type scriptedWebSocketClient struct {
	script *webSocketScript
}

func (*scriptedWebSocketClient) New(dto.Session) websocket.WebSocket { return nil }

func (c *scriptedWebSocketClient) Connect() error {
	c.script.record("Connect")
	c.script.maybePanic("Connect")
	return c.script.connectErr
}

func (c *scriptedWebSocketClient) Identify() error {
	c.script.record("Identify")
	c.script.maybePanic("Identify")
	return c.script.identifyErr
}

func (c *scriptedWebSocketClient) Session() *dto.Session {
	return c.script.identity
}

func (c *scriptedWebSocketClient) Resume() error {
	c.script.record("Resume")
	c.script.maybePanic("Resume")
	return c.script.resumeErr
}

func (c *scriptedWebSocketClient) Listening() error {
	defer close(c.script.listenExited)
	c.script.record("Listening")
	c.script.maybePanic("Listening")
	close(c.script.listenStarted)
	if c.script.onListen != nil {
		c.script.onListen(c.script.identity)
	}
	if c.script.listenErr != nil {
		return c.script.listenErr
	}
	<-c.script.closed
	return errors.New("closed")
}

func (c *scriptedWebSocketClient) Write(*dto.WSPayload) error { return nil }

func (c *scriptedWebSocketClient) Close() {
	c.script.record("Close")
	c.script.closeOnce.Do(func() { close(c.script.closed) })
}

func (s *webSocketScript) maybePanic(call string) {
	if s.panicCall == call {
		panic(call + " panic")
	}
}

func receiveCreatedSessions(t *testing.T, created <-chan dto.Session, count int) []dto.Session {
	t.Helper()
	sessions := make([]dto.Session, 0, count)
	for len(sessions) < count {
		select {
		case session := <-created:
			sessions = append(sessions, session)
		case <-time.After(time.Second):
			t.Fatalf("received %d of %d WebSocket attempts", len(sessions), count)
		}
	}
	return sessions
}

func awaitScriptListening(t *testing.T, script *webSocketScript) {
	t.Helper()
	select {
	case <-script.listenStarted:
	case <-time.After(time.Second):
		t.Fatal("WebSocket attempt did not start listening")
	}
}

func awaitScriptExit(t *testing.T, script *webSocketScript) {
	t.Helper()
	select {
	case <-script.listenExited:
	case <-time.After(time.Second):
		t.Fatal("WebSocket listener did not exit")
	}
}

func awaitSessionManagerResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("session manager did not stop")
		return nil
	}
}

var _ websocket.WebSocket = (*scriptedWebSocketFactory)(nil)
var _ websocket.WebSocket = (*scriptedWebSocketClient)(nil)
