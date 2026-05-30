# Bai 2 - Xu ly thoi gian thuc voi Webhook va Kafka

## Trang thai theo de bai

- [x] Cai dat `webhook-service` chay cong `3001`
- [x] Tao endpoint `GET /webhook` de Facebook verify webhook
- [x] Tao endpoint `POST /webhook` de nhan event do Facebook gui den
- [x] Xac thuc request bang `X-Hub-Signature-256` khi co `AppSecret`
- [x] Parse payload Facebook
- [x] Normalize event ve schema chuan chung
- [x] Dua event vao Kafka topic `raw_events`
- [x] Cung cap `docker compose` de chay Kafka
- [x] Cung cap UI de xem message Kafka
- [~] Dang ky nhan su kien binh luan tu Facebook

Muc `[~]` la phan da hoan thanh ve mat he thong backend: service da san sang cho callback URL, verify token va xu ly payload. Tuy nhien thao tac dang ky subscription phai thuc hien tren Meta for Developers bang tai khoan Facebook cua ban, nen minh da bo sung huong dan chi tiet trong file `FACEBOOK_SUBSCRIPTION.md`.

## Tom tat implementation

- Tao `webhook-service` chay cong `3001`
- Cung cap endpoint `GET /webhook` de Facebook verify webhook
- Cung cap endpoint `POST /webhook` de nhan event tu Facebook
- Xac thuc chu ky `X-Hub-Signature-256` neu co `AppSecret`
- Normalize payload Facebook thanh schema chung
- Publish du lieu vao Kafka topic `raw_events`
- Publish du lieu thong qua `Kafka REST Proxy` de tranh phu thuoc them package ben ngoai khi build local
- Chay Kafka bang `docker compose`
- Co UI xem message qua `Kafka UI`

## Tai lieu ban can dung

- `README.md`: tong quan bai lam
- `TESTING.md`: toan bo lenh test de ban tu chay
- `FACEBOOK_SUBSCRIPTION.md`: cac buoc dang ky webhook comment tren Meta Developer
- `samples/comment-event.json`: payload comment mau
- `samples/message-event.json`: payload message mau

## Cau truc

- `BaiKT4.WebhookService`: ASP.NET Core Web API nhan webhook va day du lieu vao Kafka
- `docker-compose.yml`: chay Kafka, Schema Registry, Kafka REST Proxy, Kafka UI va webhook-service

## Schema normalize

Moi event sau khi chuan hoa se co dang tong quat:

```json
{
  "eventId": "guid",
  "source": "facebook",
  "eventType": "comment.created",
  "topic": "raw_events",
  "sourceEventId": "comment_id_or_mid",
  "pageId": "page_id",
  "pageName": "page_name",
  "objectType": "page",
  "receivedAt": "2026-04-25T06:00:00+00:00",
  "occurredAt": "2026-04-25T05:59:00+00:00",
  "actor": {
    "id": "user_id",
    "name": "user_name"
  },
  "target": {
    "id": "comment_or_recipient_id",
    "parentId": "post_id"
  },
  "messageText": "Noi dung binh luan hoac tin nhan",
  "metadata": {
    "field": "feed",
    "item": "comment",
    "verb": "add"
  },
  "rawPayload": {}
}
```

## Cach chay nhanh

1. Chay toan bo he thong:

```powershell
docker compose up --build
```

2. Truy cap cac dia chi:

- Webhook service: `http://localhost:3001`
- Health check: `http://localhost:3001/health`
- Kafka REST Proxy: `http://localhost:8082`
- Kafka UI: `http://localhost:8085`

3. Facebook verify webhook:

```text
GET /webhook?hub.mode=subscribe&hub.verify_token=baitk4-dev-token&hub.challenge=123456
```

4. Chay cac lenh test trong `TESTING.md`.

## Ghi chu

- Neu `FacebookWebhook:AppSecret` rong, service cho phep payload unsigned de test local.
- Khi dua len moi truong that, can set `FacebookWebhook:AppSecret` va `AcceptUnsignedPayloads=false`.
- Trong moi truong hien tai, host khong restore duoc NuGet tu `api.nuget.org`, nen publisher duoc doi sang goi tin qua `Kafka REST Proxy` trong Docker Compose. Kafka van la Kafka that va message van xem duoc trong `Kafka UI`.
- Anh de bai hien tai bi mo va bi cat phan duoi, nen implementation nay bam chac cac muc doc duoc tren anh va bo sung them README, Dockerfile, Kafka UI de de demo.
