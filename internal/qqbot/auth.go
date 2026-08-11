package qqbot

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	qqPortalURL            = "https://q.qq.com"
	createBindTaskEndpoint = qqPortalURL + "/lite/create_bind_task"
	pollBindResultEndpoint = qqPortalURL + "/lite/poll_bind_result"
	qqConnectPageEndpoint  = qqPortalURL + "/qqbot/openclaw/connect.html"
	defaultPollInterval    = 2 * time.Second
	defaultRequestTimeout  = 10 * time.Second
	defaultMaxResponseSize = 64 << 10
	maxEncryptedSecretSize = 4 << 10
)

type bindStatus int

const (
	bindStatusNone      bindStatus = 0
	bindStatusPending   bindStatus = 1
	bindStatusCompleted bindStatus = 2
	bindStatusExpired   bindStatus = 3
)

// QRCredentials are the QQ Bot credentials returned after a successful scan.
// Callers must persist AppSecret securely and must not log it.
type QRCredentials struct {
	AppID      string
	AppSecret  string
	UserOpenID string
}

// QRConnector performs QQ's terminal QR-code binding flow.
type QRConnector struct {
	client           *http.Client
	logger           *slog.Logger
	createEndpoint   string
	pollEndpoint     string
	connectEndpoint  string
	pollInterval     time.Duration
	requestTimeout   time.Duration
	maxResponseBytes int64
	random           io.Reader
}

// NewQRConnector creates a connector for QQ's production binding service.
func NewQRConnector(client *http.Client, logger *slog.Logger) *QRConnector {
	if client == nil {
		client = http.DefaultClient
	}
	return &QRConnector{
		client:           client,
		logger:           logger,
		createEndpoint:   createBindTaskEndpoint,
		pollEndpoint:     pollBindResultEndpoint,
		connectEndpoint:  qqConnectPageEndpoint,
		pollInterval:     defaultPollInterval,
		requestTimeout:   defaultRequestTimeout,
		maxResponseBytes: defaultMaxResponseSize,
		random:           rand.Reader,
	}
}

// Connect displays each new binding URL through onQRReady and waits for a scan.
// An expired QR code is replaced automatically until binding succeeds or ctx is canceled.
func (c *QRConnector) Connect(ctx context.Context, source string, onQRReady func(string)) (QRCredentials, error) {
	if err := ctx.Err(); err != nil {
		return QRCredentials{}, err
	}
	if c == nil || c.client == nil || c.random == nil {
		return QRCredentials{}, errors.New("QQ QR connector is not initialized")
	}

	for {
		task, err := c.createBindTask(ctx)
		if err != nil {
			return QRCredentials{}, fmt.Errorf("create QQ QR binding task: %w", err)
		}
		qrURL, err := c.buildConnectURL(task.id, source)
		if err != nil {
			return QRCredentials{}, fmt.Errorf("build QQ QR binding URL: %w", err)
		}
		if onQRReady != nil {
			onQRReady(qrURL)
		}

		credentials, expired, err := c.waitForBinding(ctx, task)
		if err != nil {
			return QRCredentials{}, err
		}
		if !expired {
			return credentials, nil
		}
		if c.logger != nil {
			c.logger.Info("QQ QR binding code expired; generating a replacement")
		}
	}
}

type bindTask struct {
	id  string
	key []byte
}

func (c *QRConnector) createBindTask(ctx context.Context) (bindTask, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(c.random, key); err != nil {
		return bindTask{}, fmt.Errorf("generate encryption key: %w", err)
	}

	var response struct {
		RetCode *int   `json:"retcode"`
		Message string `json:"msg"`
		Data    *struct {
			TaskID string `json:"task_id"`
		} `json:"data"`
	}
	request := struct {
		Key string `json:"key"`
	}{Key: base64.StdEncoding.EncodeToString(key)}
	if err := c.postJSON(ctx, c.createEndpoint, request, &response); err != nil {
		return bindTask{}, err
	}
	if response.RetCode == nil {
		return bindTask{}, errors.New("create response is missing retcode")
	}
	if *response.RetCode != 0 {
		return bindTask{}, bindAPIError("create_bind_task failed", *response.RetCode, response.Message)
	}
	if response.Data == nil || strings.TrimSpace(response.Data.TaskID) == "" {
		return bindTask{}, errors.New("create response is missing data.task_id")
	}
	return bindTask{id: response.Data.TaskID, key: key}, nil
}

func (c *QRConnector) waitForBinding(ctx context.Context, task bindTask) (QRCredentials, bool, error) {
	for {
		result, err := c.pollBindResult(ctx, task.id)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return QRCredentials{}, false, ctxErr
			}
			if c.logger != nil {
				c.logger.Warn("QQ QR binding poll failed; retrying", "error", err)
			}
			if err := waitContext(ctx, c.pollInterval); err != nil {
				return QRCredentials{}, false, err
			}
			continue
		}

		switch result.status {
		case bindStatusCompleted:
			if strings.TrimSpace(result.appID) == "" {
				return QRCredentials{}, false, errors.New("completed poll response is missing data.bot_appid")
			}
			if result.encryptedSecret == "" {
				return QRCredentials{}, false, errors.New("completed poll response is missing data.bot_encrypt_secret")
			}
			secret, err := decryptBindSecret(result.encryptedSecret, task.key)
			if err != nil {
				return QRCredentials{}, false, fmt.Errorf("decrypt QQ Bot AppSecret: %w", err)
			}
			return QRCredentials{
				AppID:      result.appID,
				AppSecret:  secret,
				UserOpenID: result.userOpenID,
			}, false, nil
		case bindStatusExpired:
			return QRCredentials{}, true, nil
		case bindStatusNone, bindStatusPending:
			if err := waitContext(ctx, c.pollInterval); err != nil {
				return QRCredentials{}, false, err
			}
		default:
			if err := waitContext(ctx, c.pollInterval); err != nil {
				return QRCredentials{}, false, err
			}
		}
	}
}

type bindPollResult struct {
	status          bindStatus
	appID           string
	encryptedSecret string
	userOpenID      string
}

func (c *QRConnector) pollBindResult(ctx context.Context, taskID string) (bindPollResult, error) {
	var response struct {
		RetCode *int   `json:"retcode"`
		Message string `json:"msg"`
		Data    *struct {
			Status             *int           `json:"status"`
			BotAppID           stringOrNumber `json:"bot_appid"`
			BotEncryptedSecret string         `json:"bot_encrypt_secret"`
			UserOpenID         string         `json:"user_openid"`
		} `json:"data"`
	}
	request := struct {
		TaskID string `json:"task_id"`
	}{TaskID: taskID}
	if err := c.postJSON(ctx, c.pollEndpoint, request, &response); err != nil {
		return bindPollResult{}, err
	}
	if response.RetCode == nil {
		return bindPollResult{}, errors.New("poll response is missing retcode")
	}
	if *response.RetCode != 0 {
		return bindPollResult{}, bindAPIError("poll_bind_result failed", *response.RetCode, response.Message)
	}
	if response.Data == nil || response.Data.Status == nil {
		return bindPollResult{}, errors.New("poll response is missing data.status")
	}

	result := bindPollResult{
		status:          bindStatus(*response.Data.Status),
		appID:           string(response.Data.BotAppID),
		encryptedSecret: response.Data.BotEncryptedSecret,
		userOpenID:      response.Data.UserOpenID,
	}
	return result, nil
}

func bindAPIError(fallback string, retCode int, message string) error {
	message = strings.TrimSpace(message)
	if message == "" {
		return fmt.Errorf("%s (retcode %d)", fallback, retCode)
	}
	return fmt.Errorf("%s (retcode %d): %q", fallback, retCode, message)
}

func (c *QRConnector) postJSON(ctx context.Context, endpoint string, requestBody, responseBody any) error {
	body, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	requestCtx := ctx
	cancel := func() {}
	if c.requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, c.requestTimeout)
	}
	defer cancel()

	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	client := *c.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	response, err := client.Do(request)
	if err != nil {
		if ctxErr := requestCtx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("send request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status %d", response.StatusCode)
	}

	limit := c.maxResponseBytes
	if limit <= 0 {
		limit = defaultMaxResponseSize
	}
	responseBytes, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if int64(len(responseBytes)) > limit {
		return fmt.Errorf("response body exceeds %d bytes", limit)
	}
	if err := json.Unmarshal(responseBytes, responseBody); err != nil {
		return fmt.Errorf("decode response JSON: %w", err)
	}
	return nil
}

func (c *QRConnector) buildConnectURL(taskID, source string) (string, error) {
	parsed, err := url.Parse(c.connectEndpoint)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return "", errors.New("connect endpoint must use HTTP or HTTPS")
	}
	query := parsed.Query()
	query.Set("task_id", taskID)
	query.Set("source", source)
	query.Set("_wv", "2")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func decryptBindSecret(encryptedBase64 string, key []byte) (string, error) {
	if len(key) != 32 {
		return "", errors.New("encryption key must be exactly 32 bytes")
	}
	encrypted, err := base64.StdEncoding.Strict().DecodeString(encryptedBase64)
	if err != nil {
		return "", errors.New("invalid base64 ciphertext")
	}
	if len(encrypted) > maxEncryptedSecretSize {
		return "", fmt.Errorf("ciphertext exceeds %d bytes", maxEncryptedSecretSize)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", errors.New("invalid encryption key")
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("initialize AES-GCM: %w", err)
	}
	if len(encrypted) <= gcm.NonceSize()+gcm.Overhead() {
		return "", errors.New("ciphertext is too short")
	}
	plaintext, err := gcm.Open(nil, encrypted[:gcm.NonceSize()], encrypted[gcm.NonceSize():], nil)
	if err != nil {
		return "", errors.New("AES-GCM authentication failed")
	}
	if len(plaintext) == 0 || !utf8.Valid(plaintext) {
		return "", errors.New("decrypted AppSecret is empty or invalid UTF-8")
	}
	return string(plaintext), nil
}

type stringOrNumber string

func (value *stringOrNumber) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		*value = stringOrNumber(text)
		return nil
	}

	var number json.Number
	if err := json.Unmarshal(data, &number); err != nil {
		return errors.New("must be a string or integer")
	}
	if _, err := strconv.ParseUint(number.String(), 10, 64); err != nil {
		return errors.New("must be a string or unsigned integer")
	}
	*value = stringOrNumber(number.String())
	return nil
}

func waitContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
