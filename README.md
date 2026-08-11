# Gatus-QQBot
It is a simple QQ Bot which receives [Gatus custom alert](https://gatus.io/docs/alerting-custom) and forward to QQ user/group/channel with [botgo](https://github.com/tencent-connect/botgo) SDK

## Build
Require Go 1.23 or higher：

```sh
go build -buildvcs=false -o gatus-qqbot ./cmd/gatus-qqbot
cp config.example.toml config.toml
```

## Configuration
Refer to `config.example.toml`
```toml
[server]
address = "127.0.0.1"
port = 8080
alert_path = "/api/v1/gatus/alerts" # The path for receiving Gatus alert
auth_token = "replace-with-a-random-token"
max_body_bytes = 65536
shutdown_timeout = "10s"

[qq]
app_id = "replace-with-bot-app-id"
app_secret = "replace-with-bot-app-secret"
request_timeout = "10s"
delivery_timeout = "1m" # Queue wait plus delivery to all targets
max_pending_alerts = 64 # Waiting alerts, excluding the active delivery
gateway_ready_timeout = "30s" # Initial READY and reconnect grace period

[[qq.targets]]
name = "operations-group"
type = "group" # Or "user", "channel"
id = "replace-with-group-openid"

[message]
prefix = "[Gatus]" # Can be empty
max_length = 1800

[log]
level = "info" # debug, info, warn, error
format = "text" # text, json
```

**The secret is stored in plain text so please set strict chmod on the config file**

> **Hint：Get group ID**
>
> Setting `log.level = "debug"` and restart  
> In the target group, `@your bot` and send a message  
> The app will log `received QQ group message group_openid=...`  
> That is the group ID you want  


## Gatus Setup
Here is an example  
Refer to [Gatus Docs](https://gatus.io/docs/alerting-custom) for more info
```yaml
alerting:
  custom:
    url: "http://127.0.0.1:8080/api/v1/gatus/alerts"
    method: "POST"
    headers:
      Content-Type: "text/plain; charset=utf-8"
      Authorization: "Bearer ${GATUS_QQBOT_WEBHOOK_TOKEN}"
    body: |
      [ALERT_TRIGGERED_OR_RESOLVED]
      Endpoint: [ENDPOINT_GROUP] / [ENDPOINT_NAME]
      URL: [ENDPOINT_URL]
      Description: [ALERT_DESCRIPTION]

endpoints:
  - name: website
    group: production
    url: "https://example.com/health"
    interval: 30s
    conditions:
      - "[STATUS] == 200"
    alerts:
      - type: custom
        send-on-resolved: true
        description: "health check failed"
```

The API also supports JSON formatted call  
`state` and `endpoint_name` are mandatory

```json
{
  "state": "TRIGGERED",
  "endpoint_name": "website",
  "endpoint_group": "production",
  "endpoint_url": "https://example.com/health",
  "description": "health check failed",
  "errors": "status was 503"
}
```
But still, plain text is suggested for Gatus to inject the placeholders

## Run

To authentic with your qq bot:
```bash
./gatus-qqbot auth --config config.toml
```
Notably, the auth config will not be written to the config file  
Copy from the std output and paste them manually  

Start the service:
```bash
./gatus-qqbot serve --config config.toml
```

Here is an example systemd service file:   
```ini
[Unit]
Description=Gatus alerts to QQ Bot forwarder
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=gatus-qqbot
Group=gatus-qqbot
WorkingDirectory=/etc/gatus-qqbot
ExecStart=/usr/local/bin/gatus-qqbot serve --config /etc/gatus-qqbot/config.toml
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

## Hints
- I **don't** suggest let the app listening on public address
- This project does **not** guarantee idempotence
- You need to add your server IP to the whitelist of QQ bot platform
- Remember enabling your bot to actively send messages
