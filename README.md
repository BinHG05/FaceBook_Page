# BaiKT4 - Facebook Graph API + Webhook + Kafka Microservices

Repo nay da duoc tach theo kien truc microservice dung voi de bai:

- `backend-api` port `3000`
- `webhook-service` port `3001`
- `core-service` port `3002`
- `retry-service` port `3003`
- `prometheus` port `9090`
- `alertmanager` port `9093`

Tat ca giao tiep noi bo di qua Kafka.

## Solution structure

- `BaiKT4.Microservices.sln`: solution tong
- `BaiKT4.Contracts`: schema/event/command dung chung giua cac service
- `BaiKT4.WebhookService`: nhan webhook Facebook, verify HMAC, normalize payload, publish `raw_events`
- `BaiKT4.CoreService`: consume `raw_events`, phan loai AI, sentiment, automation rule, publish `reply_commands`
- `BaiKT4.BackendApi`: expose REST API dashboard, consume `reply_commands` va `send_retry`, kiem tra idempotency, goi Facebook Graph API, publish `send_failed`
- `BaiKT4.RetryService`: consume `send_failed`, retry exponential backoff, publish `send_retry` hoac `dead_letter`
- `monitoring/`: Prometheus, Alertmanager va local webhook receiver cho DLQ alert

## Kafka topics

- `raw_events`: webhook-service -> core-service
- `reply_commands`: core-service -> backend-api
- `send_retry`: retry-service -> backend-api
- `send_failed`: backend-api -> retry-service
- `dead_letter`: retry-service -> DLQ cho van hanh theo doi

## Luong xu ly

1. Facebook gui `POST /webhook` vao `webhook-service`
2. `webhook-service` verify chu ky, parse payload, normalize va publish `raw_events`
3. `core-service` consume `raw_events`, phat hien spam, goi AI de lay `intent` + `sentiment`, ap dung automation rule
4. `core-service` publish `reply_commands`
5. `backend-api` consume `reply_commands`, kiem tra idempotency key trong database, sau do moi goi Facebook Graph API
6. Neu gui thanh cong thi luu key da xu ly
7. Neu gui that bai thi publish `send_failed`
8. `retry-service` consume `send_failed`, doi theo exponential backoff roi publish `send_retry`
9. Neu vuot nguong retry, message duoc dua vao `dead_letter`

## Automation rules va tracking

- Spam nhe -> `hide_comment`
- Spam lap lai trong 24h -> `blacklist` noi bo
- Link doc hai / scam -> `hide_comment` + `pending_review` + dua vao `ManualReviewQueue`
- User da blacklist -> khong auto reply nua, dua sang workflow review thu cong
- Trang thai event duoc theo doi trong `ProcessedEvents`
- Trang thai command duoc theo doi trong `ProcessedCommands`
- Hang cho review thu cong duoc luu trong `ManualReviewQueue`
- Danh sach chan noi bo duoc luu trong `BlacklistedUsers`

## Monitoring

- `kafka-exporter` expose metric Kafka cho Prometheus
- Prometheus canh bao khi `dead_letter` co message moi trong 1 phut gan nhat
- Alertmanager gui webhook toi `alert-webhook`
- Co the thay receiver webhook bang Slack/Email that khi trien khai

## Yeu cau de bai da duoc map vao service nao

- Bai 1:
  - `backend-api` chua cac API proxy Facebook nhu `GET /posts`, `POST /post`, `GET /comments`
  - chỉ `backend-api` duoc phep goi Facebook Graph API
- Bai 2:
  - `webhook-service` nhan webhook va day `raw_events`
  - `core-service` xu ly thoi gian thuc
  - `retry-service` xu ly retry va DLQ
- Bai 3:
  - `core-service` goi AI phan tich intent/sentiment
  - `backend-api` co idempotency
  - `retry-service` co exponential backoff
  - `backend-api` va `core-service` deu co circuit-breaker muc co ban

## Cach chay nhanh

1. Tao file `.env` o root repo tu `.env.example` va dien `GEMINI_API_KEY`

```powershell
Copy-Item .env.example .env
```

2. Chay he thong:

```powershell
docker compose up --build
```

Sau khi chay:

- Backend API: `http://localhost:3000`
- Webhook Service: `http://localhost:3001`
- Core Service: `http://localhost:3002`
- Retry Service: `http://localhost:3003`
- Kafka REST Proxy: `http://localhost:8082`
- Kafka UI: `http://localhost:8085`
- Prometheus: `http://localhost:9090`
- Alertmanager: `http://localhost:9093`
- Alert webhook logs: `docker compose logs alert-webhook`

## Luu y

- `backend-api` hien co `SimulateMode=true` de de demo khi chua co Page Access Token that
- Muon goi Facebook Graph API that, can dien `FacebookGraph:PageId` va `FacebookGraph:PageAccessToken`
- `webhook-service` co the test local voi `AcceptUnsignedPayloads=true`
- Trong moi truong production, can dat `AcceptUnsignedPayloads=false` va cau hinh `AppSecret`
- Folder `core_service` cu khong con duoc su dung. Cau hinh hien tai nam trong `appsettings*.json` hoac bien moi truong cua tung microservice.

## Tai lieu test

- `TESTING.md`: cac lenh test nhanh
- `FACEBOOK_SUBSCRIPTION.md`: huong dan dang ky webhook tren Meta Developer
- `samples/comment-event.json`: payload comment mau
- `samples/message-event.json`: payload message mau
