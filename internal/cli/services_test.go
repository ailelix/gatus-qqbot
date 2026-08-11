package cli

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type fakeReadyService struct {
	ready chan struct{}
	run   func(context.Context) error
}

func (s *fakeReadyService) Ready() <-chan struct{} {
	return s.ready
}

func (s *fakeReadyService) Run(ctx context.Context) error {
	return s.run(ctx)
}

func TestRunServeServicesWaitsForGatewayReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gatewayStarted := make(chan struct{})
	gatewayStopped := make(chan struct{})
	gateway := &fakeReadyService{
		ready: make(chan struct{}),
		run: func(ctx context.Context) error {
			close(gatewayStarted)
			<-ctx.Done()
			close(gatewayStopped)
			return nil
		},
	}
	httpStarted := make(chan struct{})
	httpStopped := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runServeServices(ctx, gateway, time.Second, func(ctx context.Context) error {
			close(httpStarted)
			<-ctx.Done()
			close(httpStopped)
			return nil
		})
	}()

	awaitSignal(t, gatewayStarted, "gateway start")
	select {
	case <-httpStarted:
		t.Fatal("HTTP server started before Gateway READY")
	default:
	}
	close(gateway.ready)
	awaitSignal(t, httpStarted, "HTTP start")
	cancel()
	if err := awaitError(t, done); err != nil {
		t.Fatalf("runServeServices() error = %v", err)
	}
	awaitSignal(t, gatewayStopped, "gateway stop")
	awaitSignal(t, httpStopped, "HTTP stop")
}

func TestRunServeServicesReportsGatewayStartupError(t *testing.T) {
	want := errors.New("gateway failed")
	gateway := &fakeReadyService{
		ready: make(chan struct{}),
		run:   func(context.Context) error { return want },
	}
	httpCalled := false
	err := runServeServices(context.Background(), gateway, time.Second, func(context.Context) error {
		httpCalled = true
		return nil
	})
	if !errors.Is(err, want) {
		t.Fatalf("runServeServices() error = %v, want %v", err, want)
	}
	if httpCalled {
		t.Fatal("HTTP server ran after Gateway startup failed")
	}
}

func TestRunServeServicesTimesOutWaitingForReady(t *testing.T) {
	stopped := make(chan struct{})
	gateway := &fakeReadyService{
		ready: make(chan struct{}),
		run: func(ctx context.Context) error {
			<-ctx.Done()
			close(stopped)
			return context.Canceled
		},
	}
	err := runServeServices(context.Background(), gateway, 10*time.Millisecond, func(context.Context) error {
		t.Fatal("HTTP server ran without Gateway READY")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "did not become ready") {
		t.Fatalf("runServeServices() error = %v, want readiness timeout", err)
	}
	awaitSignal(t, stopped, "gateway timeout cleanup")
}

func TestRunServeServicesCancelsPeerOnRuntimeError(t *testing.T) {
	tests := []struct {
		name        string
		gatewayErr  error
		httpErr     error
		want        error
		peerStopped func(gatewayStopped, httpStopped chan struct{}) <-chan struct{}
	}{
		{
			name:    "HTTP",
			httpErr: errors.New("listen failed"),
			want:    errors.New("listen failed"),
			peerStopped: func(gatewayStopped, _ chan struct{}) <-chan struct{} {
				return gatewayStopped
			},
		},
		{
			name:       "gateway",
			gatewayErr: errors.New("connection stopped"),
			want:       errors.New("connection stopped"),
			peerStopped: func(_, httpStopped chan struct{}) <-chan struct{} {
				return httpStopped
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			triggerGateway := make(chan struct{})
			gatewayStopped := make(chan struct{})
			httpStopped := make(chan struct{})
			ready := make(chan struct{})
			close(ready)
			gateway := &fakeReadyService{
				ready: ready,
				run: func(ctx context.Context) error {
					if tt.gatewayErr != nil {
						<-triggerGateway
						return tt.gatewayErr
					}
					<-ctx.Done()
					close(gatewayStopped)
					return nil
				},
			}
			done := make(chan error, 1)
			go func() {
				done <- runServeServices(context.Background(), gateway, time.Second, func(ctx context.Context) error {
					if tt.httpErr != nil {
						return tt.httpErr
					}
					close(triggerGateway)
					<-ctx.Done()
					close(httpStopped)
					return nil
				})
			}()

			err := awaitError(t, done)
			if err == nil || !strings.Contains(err.Error(), tt.want.Error()) {
				t.Fatalf("runServeServices() error = %v, want %v", err, tt.want)
			}
			awaitSignal(t, tt.peerStopped(gatewayStopped, httpStopped), "peer cleanup")
		})
	}
}

func awaitSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func awaitError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for result")
		return nil
	}
}
