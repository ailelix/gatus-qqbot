package qqbot

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestQRConnectorConnect(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	encryptedSecret := encryptSecret(t, "test-app-secret", key)
	var createCalls, pollCalls int
	connector := testConnector(t, key, func(request *http.Request) *http.Response {
		assertJSONRequest(t, request)
		switch request.URL.Path {
		case "/create":
			createCalls++
			var body struct {
				Key string `json:"key"`
			}
			decodeRequest(t, request, &body)
			if body.Key != base64.StdEncoding.EncodeToString(key) {
				t.Fatalf("create key = %q, want deterministic 32-byte key", body.Key)
			}
			return jsonResponse(http.StatusOK, `{"retcode":0,"data":{"task_id":"task/one"}}`)
		case "/poll":
			pollCalls++
			var body struct {
				TaskID string `json:"task_id"`
			}
			decodeRequest(t, request, &body)
			if body.TaskID != "task/one" {
				t.Fatalf("poll task_id = %q", body.TaskID)
			}
			return jsonResponse(http.StatusOK, fmt.Sprintf(`{"retcode":0,"data":{"status":2,"bot_appid":"1905000000","bot_encrypt_secret":%q,"user_openid":"user-openid"}}`, encryptedSecret))
		default:
			t.Fatalf("unexpected request path %q", request.URL.Path)
			return nil
		}
	})

	var qrURL string
	credentials, err := connector.Connect(context.Background(), "gatus qqbot", func(value string) {
		qrURL = value
	})
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if credentials != (QRCredentials{AppID: "1905000000", AppSecret: "test-app-secret", UserOpenID: "user-openid"}) {
		t.Fatalf("Connect() = %+v", credentials)
	}
	if createCalls != 1 || pollCalls != 1 {
		t.Fatalf("calls = create %d, poll %d", createCalls, pollCalls)
	}

	parsed, err := url.Parse(qrURL)
	if err != nil {
		t.Fatalf("parse QR URL: %v", err)
	}
	query := parsed.Query()
	if parsed.Path != "/connect" || query.Get("task_id") != "task/one" || query.Get("source") != "gatus qqbot" || query.Get("_wv") != "2" {
		t.Fatalf("QR URL = %q", qrURL)
	}
}

func TestQRConnectorRetriesPollFailure(t *testing.T) {
	key := bytes.Repeat([]byte{0x23}, 32)
	encryptedSecret := encryptSecret(t, "secret", key)
	var pollCalls int
	connector := testConnector(t, key, func(request *http.Request) *http.Response {
		if request.URL.Path == "/create" {
			return jsonResponse(http.StatusOK, `{"retcode":0,"data":{"task_id":"task"}}`)
		}
		pollCalls++
		if pollCalls == 1 {
			return jsonResponse(http.StatusServiceUnavailable, `{"error":"temporary"}`)
		}
		return jsonResponse(http.StatusOK, fmt.Sprintf(`{"retcode":0,"data":{"status":2,"bot_appid":"app","bot_encrypt_secret":%q}}`, encryptedSecret))
	})

	credentials, err := connector.Connect(context.Background(), "", nil)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if credentials.AppSecret != "secret" || pollCalls != 2 {
		t.Fatalf("Connect() = %+v, poll calls = %d", credentials, pollCalls)
	}
}

func TestQRConnectorRefreshesExpiredQRCode(t *testing.T) {
	firstKey := bytes.Repeat([]byte{0x11}, 32)
	secondKey := bytes.Repeat([]byte{0x22}, 32)
	random := append(append([]byte(nil), firstKey...), secondKey...)
	encryptedSecret := encryptSecret(t, "refreshed-secret", secondKey)
	var createCalls, pollCalls int
	connector := testConnector(t, random, func(request *http.Request) *http.Response {
		switch request.URL.Path {
		case "/create":
			createCalls++
			return jsonResponse(http.StatusOK, fmt.Sprintf(`{"retcode":0,"data":{"task_id":"task-%d"}}`, createCalls))
		case "/poll":
			pollCalls++
			if pollCalls == 1 {
				return jsonResponse(http.StatusOK, `{"retcode":0,"data":{"status":3}}`)
			}
			return jsonResponse(http.StatusOK, fmt.Sprintf(`{"retcode":0,"data":{"status":2,"bot_appid":"app-two","bot_encrypt_secret":%q}}`, encryptedSecret))
		default:
			t.Fatalf("unexpected request path %q", request.URL.Path)
			return nil
		}
	})

	var qrURLs []string
	credentials, err := connector.Connect(context.Background(), "test", func(value string) {
		qrURLs = append(qrURLs, value)
	})
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if credentials.AppID != "app-two" || credentials.AppSecret != "refreshed-secret" {
		t.Fatalf("Connect() = %+v", credentials)
	}
	if len(qrURLs) != 2 || !strings.Contains(qrURLs[0], "task_id=task-1") || !strings.Contains(qrURLs[1], "task_id=task-2") {
		t.Fatalf("QR URLs = %#v", qrURLs)
	}
}

func TestQRConnectorHonorsContextCancellationWhilePolling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	connector := testConnector(t, bytes.Repeat([]byte{1}, 32), func(request *http.Request) *http.Response {
		if request.URL.Path == "/create" {
			return jsonResponse(http.StatusOK, `{"retcode":0,"data":{"task_id":"task"}}`)
		}
		cancel()
		return jsonResponse(http.StatusOK, `{"retcode":0,"data":{"status":1}}`)
	})
	connector.pollInterval = time.Hour

	started := time.Now()
	_, err := connector.Connect(ctx, "", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Connect() error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Connect() took %v after cancellation", elapsed)
	}
}

func TestQRConnectorRejectsOversizedResponse(t *testing.T) {
	connector := testConnector(t, bytes.Repeat([]byte{1}, 32), func(*http.Request) *http.Response {
		return jsonResponse(http.StatusOK, `{"retcode":0,"data":{"task_id":"this-is-too-large"}}`)
	})
	connector.maxResponseBytes = 16

	_, err := connector.Connect(context.Background(), "", nil)
	if err == nil || !strings.Contains(err.Error(), "response body exceeds 16 bytes") {
		t.Fatalf("Connect() error = %v", err)
	}
}

func TestQRConnectorValidatesCreateResponseFields(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "missing retcode", body: `{"data":{"task_id":"task"}}`, want: "missing retcode"},
		{name: "missing task ID", body: `{"retcode":0,"data":{}}`, want: "missing data.task_id"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connector := testConnector(t, bytes.Repeat([]byte{1}, 32), func(*http.Request) *http.Response {
				return jsonResponse(http.StatusOK, test.body)
			})
			_, err := connector.Connect(context.Background(), "", nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Connect() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestQRConnectorRetriesEveryPollError(t *testing.T) {
	tests := []struct {
		name string
		body string
		code int
	}{
		{name: "client status", code: http.StatusBadRequest},
		{name: "retcode", body: `{"retcode":123,"data":{"status":1}}`, code: http.StatusOK},
		{name: "malformed JSON", body: `{`, code: http.StatusOK},
		{name: "missing status", body: `{"retcode":0,"data":{}}`, code: http.StatusOK},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var pollCalls int
			connector := testConnector(t, bytes.Repeat([]byte{1}, 32), func(request *http.Request) *http.Response {
				if request.URL.Path == "/create" {
					return jsonResponse(http.StatusOK, `{"retcode":0,"data":{"task_id":"task"}}`)
				}
				pollCalls++
				return jsonResponse(test.code, test.body)
			})

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			_, err := connector.Connect(ctx, "", nil)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Connect() error = %v, want context deadline", err)
			}
			if pollCalls < 2 {
				t.Fatalf("poll calls = %d, want retries", pollCalls)
			}
		})
	}
}

func TestQRConnectorRejectsIncompleteCompletedResult(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "missing app ID", body: `{"retcode":0,"data":{"status":2,"bot_encrypt_secret":"value"}}`, want: "missing data.bot_appid"},
		{name: "missing secret", body: `{"retcode":0,"data":{"status":2,"bot_appid":"app"}}`, want: "missing data.bot_encrypt_secret"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var pollCalls int
			connector := testConnector(t, bytes.Repeat([]byte{1}, 32), func(request *http.Request) *http.Response {
				if request.URL.Path == "/create" {
					return jsonResponse(http.StatusOK, `{"retcode":0,"data":{"task_id":"task"}}`)
				}
				pollCalls++
				return jsonResponse(http.StatusOK, test.body)
			})

			_, err := connector.Connect(context.Background(), "", nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Connect() error = %v, want %q", err, test.want)
			}
			if pollCalls != 1 {
				t.Fatalf("poll calls = %d, want terminal failure after one call", pollCalls)
			}
		})
	}
}

func TestQRConnectorIncludesBindAPIMessage(t *testing.T) {
	connector := testConnector(t, bytes.Repeat([]byte{1}, 32), func(*http.Request) *http.Response {
		return jsonResponse(http.StatusOK, `{"retcode":123,"msg":"binding not allowed\ncontact support"}`)
	})
	_, err := connector.Connect(context.Background(), "", nil)
	if err == nil || !strings.Contains(err.Error(), `"binding not allowed\ncontact support"`) || !strings.Contains(err.Error(), "retcode 123") {
		t.Fatalf("Connect() error = %v", err)
	}
}

func TestQRConnectorTreatsUnknownStatusAsPending(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	encryptedSecret := encryptSecret(t, "secret", key)
	var pollCalls int
	connector := testConnector(t, key, func(request *http.Request) *http.Response {
		if request.URL.Path == "/create" {
			return jsonResponse(http.StatusOK, `{"retcode":0,"data":{"task_id":"task"}}`)
		}
		pollCalls++
		if pollCalls == 1 {
			return jsonResponse(http.StatusOK, `{"retcode":0,"data":{"status":99}}`)
		}
		return jsonResponse(http.StatusOK, fmt.Sprintf(`{"retcode":0,"data":{"status":2,"bot_appid":"app","bot_encrypt_secret":%q}}`, encryptedSecret))
	})

	credentials, err := connector.Connect(context.Background(), "", nil)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if credentials.AppSecret != "secret" || pollCalls != 2 {
		t.Fatalf("Connect() = %+v, poll calls = %d", credentials, pollCalls)
	}
}

func TestQRConnectorRejectsUnauthenticatedCiphertext(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	connector := testConnector(t, key, func(request *http.Request) *http.Response {
		if request.URL.Path == "/create" {
			return jsonResponse(http.StatusOK, `{"retcode":0,"data":{"task_id":"task"}}`)
		}
		ciphertext := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 29))
		return jsonResponse(http.StatusOK, fmt.Sprintf(`{"retcode":0,"data":{"status":2,"bot_appid":"app","bot_encrypt_secret":%q}}`, ciphertext))
	})

	_, err := connector.Connect(context.Background(), "", nil)
	if err == nil || !strings.Contains(err.Error(), "AES-GCM authentication failed") {
		t.Fatalf("Connect() error = %v", err)
	}
}

func TestQRConnectorAcceptsNumericBotAppID(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	encryptedSecret := encryptSecret(t, "secret", key)
	connector := testConnector(t, key, func(request *http.Request) *http.Response {
		if request.URL.Path == "/create" {
			return jsonResponse(http.StatusOK, `{"retcode":0,"data":{"task_id":"task"}}`)
		}
		return jsonResponse(http.StatusOK, fmt.Sprintf(`{"retcode":0,"data":{"status":2,"bot_appid":1905000000,"bot_encrypt_secret":%q}}`, encryptedSecret))
	})

	credentials, err := connector.Connect(context.Background(), "", nil)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if credentials.AppID != "1905000000" {
		t.Fatalf("AppID = %q", credentials.AppID)
	}
}

func TestQRConnectorDoesNotFollowRedirectsOrMutateClient(t *testing.T) {
	var followed, callerPolicyUsed bool
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/create" {
			response := jsonResponse(http.StatusTemporaryRedirect, "")
			response.Header.Set("Location", "https://leak.test/disclose")
			return response, nil
		}
		followed = true
		return jsonResponse(http.StatusOK, `{}`), nil
	})
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			callerPolicyUsed = true
			return nil
		},
	}
	connector := configureTestConnector(NewQRConnector(client, nil), bytes.Repeat([]byte{1}, 32))

	_, err := connector.Connect(context.Background(), "", nil)
	if err == nil || !strings.Contains(err.Error(), "unexpected HTTP status 307") {
		t.Fatalf("Connect() error = %v", err)
	}
	if followed || callerPolicyUsed {
		t.Fatalf("redirect followed = %v, caller policy used = %v", followed, callerPolicyUsed)
	}
	if client.CheckRedirect == nil {
		t.Fatal("connector mutated the caller's HTTP client")
	}
}

func TestDecryptBindSecretValidatesLengths(t *testing.T) {
	if _, err := decryptBindSecret(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 29)), bytes.Repeat([]byte{1}, 31)); err == nil || !strings.Contains(err.Error(), "exactly 32 bytes") {
		t.Fatalf("short key error = %v", err)
	}
	if _, err := decryptBindSecret(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 28)), bytes.Repeat([]byte{1}, 32)); err == nil || !strings.Contains(err.Error(), "too short") {
		t.Fatalf("empty payload error = %v", err)
	}
	if _, err := decryptBindSecret(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, maxEncryptedSecretSize+1)), bytes.Repeat([]byte{1}, 32)); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized payload error = %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func testConnector(t *testing.T, random []byte, handler func(*http.Request) *http.Response) *QRConnector {
	t.Helper()
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response := handler(request)
		if response == nil {
			return nil, errors.New("test handler returned nil response")
		}
		return response, nil
	})}
	return configureTestConnector(NewQRConnector(client, nil), random)
}

func configureTestConnector(connector *QRConnector, random []byte) *QRConnector {
	connector.createEndpoint = "https://qq.test/create"
	connector.pollEndpoint = "https://qq.test/poll"
	connector.connectEndpoint = "https://qq.test/connect"
	connector.pollInterval = time.Millisecond
	connector.random = bytes.NewReader(random)
	return connector
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Header:        http.Header{"Content-Type": []string{"application/json"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

func assertJSONRequest(t *testing.T, request *http.Request) {
	t.Helper()
	if request.Method != http.MethodPost {
		t.Fatalf("method = %s, want POST", request.Method)
	}
	if request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Accept") != "application/json" {
		t.Fatalf("headers = %#v", request.Header)
	}
	if request.ContentLength <= 0 {
		t.Fatalf("Content-Length = %d", request.ContentLength)
	}
}

func decodeRequest(t *testing.T, request *http.Request, target any) {
	t.Helper()
	if err := json.NewDecoder(request.Body).Decode(target); err != nil {
		t.Fatalf("decode request: %v", err)
	}
}

func encryptSecret(t *testing.T, secret string, key []byte) string {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("aes.NewCipher(): %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("cipher.NewGCM(): %v", err)
	}
	nonce := bytes.Repeat([]byte{0x7a}, gcm.NonceSize())
	ciphertext := gcm.Seal(nil, nonce, []byte(secret), nil)
	return base64.StdEncoding.EncodeToString(append(nonce, ciphertext...))
}
