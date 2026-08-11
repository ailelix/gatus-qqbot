package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ailelix/gatus-qqbot/internal/delivery"
)

type fakeSender struct {
	messages []string
	err      error
}

func (f *fakeSender) Send(_ context.Context, content string) error {
	f.messages = append(f.messages, content)
	return f.err
}

func TestAlertHandlerAcceptsAndForwards(t *testing.T) {
	sender := &fakeSender{}
	handler := testHandler(sender)
	recorder := request(t, handler, http.MethodPost, "/api/v1/gatus/alerts", validBody, "secret", "application/json")

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if len(sender.messages) != 1 || !strings.Contains(sender.messages[0], "Endpoint: production / website") {
		t.Fatalf("messages = %v", sender.messages)
	}
}

func TestAlertHandlerAcceptsLiteralTextFromGatus(t *testing.T) {
	sender := &fakeSender{}
	body := "TRIGGERED\nDescription: quoted \"value\" at C:\\health\nErrors: first\nsecond"
	recorder := request(t, testHandler(sender), http.MethodPost, "/api/v1/gatus/alerts", body, "secret", "text/plain; charset=utf-8")

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	want := "[Gatus]\n" + body
	if len(sender.messages) != 1 || sender.messages[0] != want {
		t.Fatalf("messages = %q, want %q", sender.messages, want)
	}
}

func TestAlertHandlerLogsTextMetadataWithoutAlertContent(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	handler := NewHandler(HandlerOptions{
		AlertPath:       "/api/v1/gatus/alerts",
		AuthToken:       "secret",
		MaxBodyBytes:    1024,
		MessageMaxRunes: 1800,
		Logger:          logger,
	}, &fakeSender{})
	body := "RESOLVED\nEndpoint: production / website\nErrors: private diagnostic"
	recorder := request(t, handler, http.MethodPost, "/api/v1/gatus/alerts", body, "secret", "text/plain")

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	logOutput := output.String()
	for _, want := range []string{"endpoint=\"production / website\"", "state=RESOLVED"} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("text alert log does not contain %q: %s", want, logOutput)
		}
	}
	if strings.Contains(logOutput, "private diagnostic") {
		t.Fatalf("text alert log leaked message content: %s", logOutput)
	}
}

func TestAlertHandlerOmitsUnavailableTextMetadata(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	handler := NewHandler(HandlerOptions{
		AlertPath:       "/api/v1/gatus/alerts",
		MaxBodyBytes:    1024,
		MessageMaxRunes: 1800,
		Logger:          logger,
	}, &fakeSender{})
	recorder := request(t, handler, http.MethodPost, "/api/v1/gatus/alerts", "arbitrary alert text", "", "text/plain")

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	logOutput := output.String()
	for _, emptyField := range []string{"endpoint=", "state="} {
		if strings.Contains(logOutput, emptyField) {
			t.Fatalf("text alert log contains unavailable field %q: %s", emptyField, logOutput)
		}
	}
}

func TestAlertHandlerRejectsUnauthorizedRequest(t *testing.T) {
	sender := &fakeSender{}
	recorder := request(t, testHandler(sender), http.MethodPost, "/api/v1/gatus/alerts", validBody, "wrong", "application/json")
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", recorder.Code)
	}
	if len(sender.messages) != 0 {
		t.Fatalf("messages = %v, want none", sender.messages)
	}
}

func TestAlertHandlerAllowsMissingAuthorizationWhenTokenIsDisabled(t *testing.T) {
	sender := &fakeSender{}
	handler := NewHandler(HandlerOptions{
		AlertPath:       "/api/v1/gatus/alerts",
		MaxBodyBytes:    1024,
		MessagePrefix:   "[Gatus]",
		MessageMaxRunes: 1800,
		Logger:          discardLogger(),
	}, sender)
	recorder := request(t, handler, http.MethodPost, "/api/v1/gatus/alerts", validBody, "", "application/json")
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestAlertHandlerValidatesProtocolAndBody(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		contentType string
		body        string
		wantStatus  int
	}{
		{name: "method", method: http.MethodGet, contentType: "application/json", body: validBody, wantStatus: 405},
		{name: "content type", method: http.MethodPost, contentType: "application/xml", body: validBody, wantStatus: 415},
		{name: "text charset", method: http.MethodPost, contentType: "text/plain; charset=iso-8859-1", body: "alert", wantStatus: 415},
		{name: "malformed", method: http.MethodPost, contentType: "application/json", body: `{`, wantStatus: 400},
		{name: "unknown field", method: http.MethodPost, contentType: "application/json", body: `{"state":"TRIGGERED","endpoint_name":"api","extra":true}`, wantStatus: 400},
		{name: "trailing value", method: http.MethodPost, contentType: "application/json", body: validBody + `{}`, wantStatus: 400},
		{name: "missing field", method: http.MethodPost, contentType: "application/json", body: `{}`, wantStatus: 422},
		{name: "too large", method: http.MethodPost, contentType: "application/json", body: `{"state":"TRIGGERED","endpoint_name":"` + strings.Repeat("x", 500) + `"}`, wantStatus: 413},
		{name: "empty text", method: http.MethodPost, contentType: "text/plain", body: " \n\t", wantStatus: 422},
		{name: "invalid text", method: http.MethodPost, contentType: "text/plain", body: string([]byte{0xff}), wantStatus: 422},
		{name: "text too large", method: http.MethodPost, contentType: "text/plain", body: strings.Repeat("x", 500), wantStatus: 413},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender := &fakeSender{}
			recorder := request(t, testHandler(sender), tt.method, "/api/v1/gatus/alerts", tt.body, "secret", tt.contentType)
			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", recorder.Code, tt.wantStatus, recorder.Body.String())
			}
			if len(sender.messages) != 0 {
				t.Fatalf("messages = %v, want none", sender.messages)
			}
		})
	}
}

func TestAlertHandlerReportsDeliveryFailure(t *testing.T) {
	sender := &fakeSender{err: errors.New("QQ unavailable")}
	recorder := request(t, testHandler(sender), http.MethodPost, "/api/v1/gatus/alerts", validBody, "secret", "application/json")
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", recorder.Code)
	}
}

func TestAlertHandlerReportsFullDeliveryQueue(t *testing.T) {
	sender := &fakeSender{err: delivery.ErrQueueFull}
	recorder := request(t, testHandler(sender), http.MethodPost, "/api/v1/gatus/alerts", validBody, "secret", "application/json")
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
	if got := recorder.Header().Get("Retry-After"); got != queueRetryAfter {
		t.Fatalf("Retry-After = %q, want %q", got, queueRetryAfter)
	}
	if recorder.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", recorder.Header().Get("Content-Type"))
	}
}

func TestUnknownPathReturnsJSONNotFound(t *testing.T) {
	recorder := request(t, testHandler(&fakeSender{}), http.MethodGet, "/not-found", "", "", "")
	if recorder.Code != http.StatusNotFound || recorder.Header().Get("Content-Type") != "application/json" ||
		recorder.Body.String() != "{\"error\":\"not found\"}\n" {
		t.Fatalf("not-found response = %d %q", recorder.Code, recorder.Body.String())
	}
}

type blockingSender struct {
	entered chan struct{}
}

func (s blockingSender) Send(ctx context.Context, _ string) error {
	close(s.entered)
	<-ctx.Done()
	return ctx.Err()
}

func TestDeliveryOutlivesRequestButStopsWithService(t *testing.T) {
	serviceCtx, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	entered := make(chan struct{})
	handler := NewHandler(HandlerOptions{
		AlertPath:       "/api/v1/gatus/alerts",
		AuthToken:       "secret",
		MaxBodyBytes:    1024,
		MessageMaxRunes: 1800,
		DeliveryContext: serviceCtx,
		DeliveryTimeout: time.Second,
		Logger:          discardLogger(),
	}, blockingSender{entered: entered})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/gatus/alerts", strings.NewReader(validBody)).WithContext(requestCtx)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, req)
		close(done)
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("delivery did not start")
	}
	cancelRequest()
	select {
	case <-done:
		t.Fatal("request cancellation stopped delivery")
	case <-time.After(50 * time.Millisecond):
	}
	cancelService()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("service cancellation did not stop delivery")
	}
}

func TestDeliveryTimeoutReturnsGatewayTimeout(t *testing.T) {
	entered := make(chan struct{})
	handler := NewHandler(HandlerOptions{
		AlertPath:       "/api/v1/gatus/alerts",
		AuthToken:       "secret",
		MaxBodyBytes:    1024,
		MessageMaxRunes: 1800,
		DeliveryTimeout: 10 * time.Millisecond,
		Logger:          discardLogger(),
	}, blockingSender{entered: entered})
	recorder := request(t, handler, http.MethodPost, "/api/v1/gatus/alerts", validBody, "secret", "application/json")
	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", recorder.Code)
	}
}

func testHandler(sender Sender) http.Handler {
	return NewHandler(HandlerOptions{
		AlertPath:       "/api/v1/gatus/alerts",
		AuthToken:       "secret",
		MaxBodyBytes:    256,
		MessagePrefix:   "[Gatus]",
		MessageMaxRunes: 1800,
		Logger:          discardLogger(),
	}, sender)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func request(t *testing.T, handler http.Handler, method, path, body, token, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder
}

const validBody = `{
  "state":"TRIGGERED",
  "endpoint_name":"website",
  "endpoint_group":"production",
  "endpoint_url":"https://example.com/health",
  "description":"health check failed",
  "errors":"status was 503"
}`
