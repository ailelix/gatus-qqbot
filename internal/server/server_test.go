package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRunStopsForCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := Run(ctx, "127.0.0.1:0", http.NewServeMux(), time.Second, logger); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestRunReportsListenError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := Run(context.Background(), "127.0.0.1:-1", http.NewServeMux(), time.Second, logger)
	if err == nil || !strings.Contains(err.Error(), "listen on") {
		t.Fatalf("Run() error = %v, want listen error", err)
	}
}

func TestRunStopsAfterServing(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			t.Skipf("socket creation is not permitted in this environment: %v", err)
		}
		t.Fatalf("reserve test address: %v", err)
	}
	address := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatalf("release test address: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, address, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}), time.Second, logger)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		response, requestErr := http.Get("http://" + address)
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode != http.StatusNoContent {
				t.Fatalf("status = %d, want 204", response.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not start: %v", requestErr)
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not stop after cancellation")
	}
}

func TestRunGracefullyDrainsActiveHandler(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			t.Skipf("socket creation is not permitted in this environment: %v", err)
		}
		t.Fatalf("reserve test address: %v", err)
	}
	address := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatalf("release test address: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, address, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(entered)
			<-release
			w.WriteHeader(http.StatusNoContent)
		}), time.Second, logger)
	}()

	responseDone := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for {
			response, requestErr := http.Get("http://" + address)
			if requestErr == nil {
				_ = response.Body.Close()
				if response.StatusCode != http.StatusNoContent {
					responseDone <- fmt.Errorf("status = %d, want 204", response.StatusCode)
					return
				}
				responseDone <- nil
				return
			}
			if time.Now().After(deadline) {
				responseDone <- fmt.Errorf("server did not start: %w", requestErr)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	select {
	case <-entered:
	case err := <-responseDone:
		t.Fatalf("request ended before entering handler: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("request did not enter handler")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run() returned before active handler completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-responseDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not finish after active handler completed")
	}
}
