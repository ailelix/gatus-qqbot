package qqbot

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ailelix/gatus-qqbot/internal/config"
	"github.com/ailelix/gatus-qqbot/internal/delivery"
	"github.com/tencent-connect/botgo/dto"
	"github.com/tencent-connect/botgo/openapi/options"
)

type fakeMessageAPI struct {
	mu        sync.Mutex
	calls     []string
	failID    string
	delay     time.Duration
	active    atomic.Int32
	maxActive atomic.Int32
	entered   chan struct{}
	release   chan struct{}
	afterCall func()
}

func (f *fakeMessageAPI) PostMessage(ctx context.Context, id string, msg *dto.MessageToCreate, _ ...options.Option) (*dto.Message, error) {
	return f.record(ctx, "channel", id, msg.Content)
}

func (f *fakeMessageAPI) PostGroupMessage(ctx context.Context, id string, msg dto.APIMessage, _ ...options.Option) (*dto.Message, error) {
	return f.record(ctx, "group", id, msg.(*dto.MessageToCreate).Content)
}

func (f *fakeMessageAPI) PostC2CMessage(ctx context.Context, id string, msg dto.APIMessage, _ ...options.Option) (*dto.Message, error) {
	return f.record(ctx, "user", id, msg.(*dto.MessageToCreate).Content)
}

func (f *fakeMessageAPI) record(ctx context.Context, kind, id, content string) (*dto.Message, error) {
	active := f.active.Add(1)
	defer f.active.Add(-1)
	for active > f.maxActive.Load() && !f.maxActive.CompareAndSwap(f.maxActive.Load(), active) {
	}
	if f.entered != nil {
		select {
		case f.entered <- struct{}{}:
		default:
		}
	}
	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	} else if f.delay > 0 {
		timer := time.NewTimer(f.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, kind+":"+id+":"+content)
	if f.afterCall != nil {
		f.afterCall()
	}
	if id == f.failID {
		return nil, errors.New("API rejected message")
	}
	return &dto.Message{ID: "message-id"}, nil
}

func (f *fakeMessageAPI) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func TestSenderDispatchesEveryTarget(t *testing.T) {
	api := &fakeMessageAPI{}
	sender := NewSender(api, []config.TargetConfig{
		{Type: "channel", ID: "c"},
		{Type: "group", ID: "g"},
		{Type: "user", ID: "u"},
	}, 8, nil)

	if err := sender.Send(context.Background(), "alert"); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	want := []string{"channel:c:alert", "group:g:alert", "user:u:alert"}
	if strings.Join(api.calls, ",") != strings.Join(want, ",") {
		t.Fatalf("calls = %v, want %v", api.calls, want)
	}
}

func TestSenderContinuesAfterFailure(t *testing.T) {
	api := &fakeMessageAPI{failID: "g"}
	sender := NewSender(api, []config.TargetConfig{
		{Type: "group", ID: "g", Name: "operators"},
		{Type: "user", ID: "u"},
	}, 8, nil)

	err := sender.Send(context.Background(), "alert")
	if err == nil || !strings.Contains(err.Error(), "deliver to operators") {
		t.Fatalf("Send() error = %v, want named target error", err)
	}
	if api.callCount() != 2 {
		t.Fatalf("calls = %v, want both targets attempted", api.calls)
	}
}

func TestSenderSerializesConcurrentAlerts(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	api := &fakeMessageAPI{entered: entered, release: release}
	sender := NewSender(api, []config.TargetConfig{{Type: "group", ID: "g"}}, 8, nil)
	firstDone := make(chan error, 1)
	go func() { firstDone <- sender.Send(context.Background(), "first") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first Send() did not reach the API")
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- sender.Send(context.Background(), "second") }()
	select {
	case err := <-secondDone:
		t.Fatalf("concurrent Send() returned before the first delivery finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Send() error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Send() error = %v", err)
	}
	if got := api.callCount(); got != 2 {
		t.Fatalf("API call count = %d, want both sends", got)
	}
	if got := api.maxActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent API calls = %d, want 1", got)
	}
}

func TestSenderStopsWaitingWhenContextIsCanceled(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	api := &fakeMessageAPI{entered: entered, release: release}
	sender := NewSender(api, []config.TargetConfig{{Type: "group", ID: "g"}}, 8, nil)
	firstDone := make(chan error, 1)
	go func() { firstDone <- sender.Send(context.Background(), "first") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first Send() did not reach the API")
	}
	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() { secondDone <- sender.Send(ctx, "second") }()
	cancel()
	if err := <-secondDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Send() error = %v, want context.Canceled", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Send() error = %v", err)
	}
	if got := api.callCount(); got != 1 {
		t.Fatalf("API call count = %d, want 1", got)
	}
}

func TestSenderDoesNotUseIdleAPIWithCanceledContext(t *testing.T) {
	api := &fakeMessageAPI{}
	sender := NewSender(api, []config.TargetConfig{{Type: "group", ID: "g"}}, 8, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sender.Send(ctx, "alert"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Send() error = %v, want context.Canceled", err)
	}
	if got := api.callCount(); got != 0 {
		t.Fatalf("API call count = %d, want 0", got)
	}
}

func TestSenderRejectsAlertsBeyondPendingLimit(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	api := &fakeMessageAPI{entered: entered, release: release}
	sender := NewSender(api, []config.TargetConfig{{Type: "group", ID: "g"}}, 1, nil)
	firstDone := make(chan error, 1)
	go func() { firstDone <- sender.Send(context.Background(), "first") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first Send() did not reach the API")
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- sender.Send(context.Background(), "second") }()
	deadline := time.Now().Add(time.Second)
	for len(sender.admission) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("second Send() did not enter the pending queue")
		}
		time.Sleep(time.Millisecond)
	}
	if err := sender.Send(context.Background(), "third"); !errors.Is(err, delivery.ErrQueueFull) {
		t.Fatalf("third Send() error = %v, want ErrQueueFull", err)
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Send() error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Send() error = %v", err)
	}
	if got := api.callCount(); got != 2 {
		t.Fatalf("API call count = %d, want 2", got)
	}
}

func TestSenderRejectsWaitingAlertWhenPendingLimitIsZero(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	api := &fakeMessageAPI{entered: entered, release: release}
	sender := NewSender(api, []config.TargetConfig{{Type: "group", ID: "g"}}, 0, nil)
	firstDone := make(chan error, 1)
	go func() { firstDone <- sender.Send(context.Background(), "first") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first Send() did not reach the API")
	}

	if err := sender.Send(context.Background(), "second"); !errors.Is(err, delivery.ErrQueueFull) {
		t.Fatalf("second Send() error = %v, want ErrQueueFull", err)
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Send() error = %v", err)
	}
	if got := api.callCount(); got != 1 {
		t.Fatalf("API call count = %d, want 1", got)
	}
}

func TestSenderChecksContextBeforeEveryTarget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	api := &fakeMessageAPI{afterCall: cancel}
	sender := NewSender(api, []config.TargetConfig{
		{Type: "group", ID: "first"},
		{Type: "group", ID: "second"},
	}, 8, nil)

	err := sender.Send(ctx, "alert")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Send() error = %v, want context.Canceled", err)
	}
	if got := api.callCount(); got != 1 {
		t.Fatalf("API call count = %d, want 1", got)
	}
}

func TestSenderPreservesDeadlineWhenAPIFailsAfterTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	api := &fakeMessageAPI{
		failID: "first",
		afterCall: func() {
			<-ctx.Done()
		},
	}
	sender := NewSender(api, []config.TargetConfig{
		{Type: "group", ID: "first"},
		{Type: "group", ID: "second"},
	}, 8, nil)

	err := sender.Send(ctx, "alert")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send() error = %v, want context.DeadlineExceeded", err)
	}
	if !strings.Contains(err.Error(), "API rejected message") {
		t.Fatalf("Send() error = %v, want API delivery error", err)
	}
	if got := api.callCount(); got != 1 {
		t.Fatalf("API call count = %d, want 1", got)
	}
}
