package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/ailelix/gatus-qqbot/internal/delivery"
	"github.com/ailelix/gatus-qqbot/internal/gatus"
)

const (
	defaultDeliveryTimeout = time.Minute
	queueRetryAfter        = "5"
)

type Sender interface {
	Send(ctx context.Context, content string) error
}

type HandlerOptions struct {
	AlertPath       string
	AuthToken       string
	MaxBodyBytes    int64
	MessagePrefix   string
	MessageMaxRunes int
	DeliveryContext context.Context
	DeliveryTimeout time.Duration
	Logger          *slog.Logger
}

func NewHandler(options HandlerOptions, sender Sender) http.Handler {
	if options.DeliveryContext == nil {
		options.DeliveryContext = context.Background()
	}
	if options.DeliveryTimeout <= 0 {
		options.DeliveryTimeout = defaultDeliveryTimeout
	}
	h := &handler{options: options, sender: sender}
	mux := http.NewServeMux()
	mux.HandleFunc(options.AlertPath, h.alert)
	mux.HandleFunc("/", h.notFound)
	return mux
}

type handler struct {
	options HandlerOptions
	sender  Sender
}

func (h *handler) notFound(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotFound, "not found")
}

func (h *handler) alert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !h.authorized(r) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	mediaType, parameters, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	charset := strings.ToLower(parameters["charset"])
	if err != nil || (charset != "" && charset != "utf-8") || (mediaType != "application/json" && mediaType != "text/plain") {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json or UTF-8 text/plain")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, h.options.MaxBodyBytes)

	var message, endpoint, state string
	if mediaType == "application/json" {
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var alert gatus.Alert
		if err := decoder.Decode(&alert); err != nil {
			h.writeDecodeError(w, err)
			return
		}
		if err := ensureEOF(decoder); err != nil {
			h.writeDecodeError(w, err)
			return
		}
		if err := alert.Validate(); err != nil {
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		message = gatus.Format(alert, h.options.MessagePrefix, h.options.MessageMaxRunes)
		endpoint, state = alert.LogValue(), alert.State
	} else {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			h.writeDecodeError(w, err)
			return
		}
		message, err = gatus.FormatText(string(body), h.options.MessagePrefix, h.options.MessageMaxRunes)
		if err != nil {
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		endpoint, state = gatus.TextMetadata(string(body))
	}

	deliveryCtx, cancel := context.WithTimeout(h.options.DeliveryContext, h.options.DeliveryTimeout)
	defer cancel()
	if err := h.sender.Send(deliveryCtx, message); err != nil {
		attrs := alertLogAttrs(endpoint, state)
		if errors.Is(err, delivery.ErrQueueFull) {
			h.logger().Warn("rejected Gatus alert because the QQ delivery queue is full", attrs...)
			w.Header().Set("Retry-After", queueRetryAfter)
			writeError(w, http.StatusServiceUnavailable, "QQ alert delivery queue is full")
			return
		}
		attrs = append(attrs, "error", err)
		h.logger().Error("failed to forward Gatus alert", attrs...)
		if errors.Is(err, context.DeadlineExceeded) {
			writeError(w, http.StatusGatewayTimeout, "QQ alert delivery timed out")
			return
		}
		writeError(w, http.StatusBadGateway, "failed to forward alert to one or more QQ targets")
		return
	}
	attrs := alertLogAttrs(endpoint, state)
	attrs = append(attrs, "content_type", mediaType)
	h.logger().Info("forwarded Gatus alert", attrs...)
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func alertLogAttrs(endpoint, state string) []any {
	attrs := make([]any, 0, 4)
	if endpoint != "" {
		attrs = append(attrs, "endpoint", endpoint)
	}
	if state != "" {
		attrs = append(attrs, "state", state)
	}
	return attrs
}

func (h *handler) authorized(r *http.Request) bool {
	if h.options.AuthToken == "" {
		return true
	}
	want := []byte("Bearer " + h.options.AuthToken)
	got := []byte(r.Header.Get("Authorization"))
	return subtle.ConstantTimeCompare(got, want) == 1
}

func (h *handler) writeDecodeError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
		return
	}
	writeError(w, http.StatusBadRequest, "invalid request body")
}

func (h *handler) logger() *slog.Logger {
	if h.options.Logger == nil {
		return slog.Default()
	}
	return h.options.Logger
}

func ensureEOF(decoder *json.Decoder) error {
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
