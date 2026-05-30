# Huong dan test BaiKT4

File nay ghi lai dung thu tu de test bai khi can dung `ngrok`.

## 1. Luu y quan trong ve cong `3001`

Khong chay `webhook-service` trong Docker neu ban dinh chay bang `dotnet run` o may local.

Ly do:

- `webhook-service` trong `docker-compose.yml` dang map `3001:8080`
- profile local cua `.NET` cung chay `http://localhost:3001`

Neu chay ca hai cung luc se bi trung cong `3001`.

Vi vay, khi test bang `ngrok`, hay:

- chi chay Kafka stack trong Docker
- chay `webhook-service` bang `dotnet run` o may local

## 2. Chay Kafka va cac service ho tro bang Docker

Chay trong thu muc `BaiKT4`:

```powershell
docker compose up kafka schema-registry kafka-rest-proxy kafka-ui
```

Sau khi lenh chay xong, cac dia chi can dung:

- Kafka REST Proxy: `http://localhost:8082`
- Kafka UI: `http://localhost:8085`

## 3. Chay webhook-service bang local

Mo terminal khac, chay trong thu muc `BaiKT4/BaiKT4.WebhookService`:

```powershell
dotnet build
dotnet run
```

Service se chay o:

- `http://localhost:3001`

## 4. Kiem tra health endpoint

```powershell
curl http://localhost:3001/health
```

Ket qua mong doi: JSON co `status = healthy`.

## 5. Kiem tra verify webhook local

```powershell
curl "http://localhost:3001/webhook?hub.mode=subscribe&hub.verify_token=baitk4-dev-token&hub.challenge=123456"
```

Ket qua mong doi: tra ve chuoi:

```text
123456
```

Neu ban mo truc tiep `http://localhost:3001/webhook` hoac URL `ngrok` ma khong co query string, service co the tra:

```json
{"error":"Invalid hub.mode. Expected 'subscribe'."}
```

Day la hanh vi binh thuong, khong phai loi.

## 6. Chay ngrok de public cong `3001`

Mo terminal thu ba:

```powershell
ngrok http 3001
```

Ban se thay dong gan giong:

```text
Forwarding  https://abc123.ngrok-free.app -> http://localhost:3001
```

Luc nay:

- local webhook URL: `http://localhost:3001/webhook`
- public webhook URL: `https://abc123.ngrok-free.app/webhook`

Hay thay `abc123.ngrok-free.app` bang domain that ma `ngrok` cap cho ban.

Web interface cua `ngrok`:

- `http://127.0.0.1:4040`

Tai day ban co the xem request, header, query string va body di qua `ngrok`.

## 7. Kiem tra verify webhook qua ngrok

Test nhanh bang browser hoac `curl`:

```powershell
curl "https://<domain-ngrok>/webhook?hub.mode=subscribe&hub.verify_token=baitk4-dev-token&hub.challenge=123456"
```

Vi du:

```powershell
curl "https://abc123.ngrok-free.app/webhook?hub.mode=subscribe&hub.verify_token=baitk4-dev-token&hub.challenge=123456"
```

Ket qua mong doi: tra ve chuoi `123456`.

## 8. Cau hinh tren Meta Developer

Trong phan `Webhooks` cua `Page/Trang`, nhap:

- `Callback URL`: `https://<domain-ngrok>/webhook`
- `Verify token`: `baitk4-dev-token`

Luu y:

- Khong nhap `http://localhost:3001/webhook`
- Khong tu mo `/webhook` tron roi thay `400` ma nghi la loi

Meta se tu dong goi request verify co dang:

```text
GET /webhook?hub.mode=subscribe&hub.verify_token=...&hub.challenge=...
```

Sau khi verify xong, o phan chon field su kien, uu tien chon:

- `feed`

Neu muon test them message thi co the chon them:

- `messages`
- `messaging_postbacks`

## 9. Gui su kien comment mau de test local nhanh

Chay trong thu muc `BaiKT4`:

```powershell
curl -X POST http://localhost:3001/webhook `
  -H "Content-Type: application/json" `
  --data-binary "@samples/comment-event.json"
```

Ket qua mong doi:

- API tra ve `received = true`
- `publishedCount` lon hon `0`

## 10. Gui su kien message mau

Chay trong thu muc `BaiKT4`:

```powershell
curl -X POST http://localhost:3001/webhook `
  -H "Content-Type: application/json" `
  --data-binary "@samples/message-event.json"
```

Ket qua mong doi:

- API tra ve `received = true`
- `publishedCount` lon hon `0`

## 11. Test bang Facebook that

Sau khi da cau hinh `Callback URL` va `Verify token` tren Meta:

1. Tao comment moi tren bai viet cua `Page`
2. Facebook se gui webhook ve URL `ngrok`
3. `webhook-service` nhan event va normalize
4. Event duoc publish vao topic `raw_events`

Neu can xem request Facebook vua gui, mo:

- `http://127.0.0.1:4040`

## 12. Xem message trong Kafka UI

1. Mo `http://localhost:8085`
2. Chon cluster `baitk4-local`
3. Mo topic `raw_events`
4. Xem danh sach message da duoc publish

Ban se thay cac field nhu:

- `eventId`
- `eventType`
- `pageId`
- `actor`
- `target`
- `messageText`
- `metadata`
- `rawPayload`

## 13. Test xac thuc chu ky Facebook

Mac dinh project dang de:

- `FacebookWebhook:AppSecret = ""`
- `AcceptUnsignedPayloads = true`

Neu muon test chu ky that:

1. Sua `AppSecret` trong `appsettings.Development.json` hoac bien moi truong
2. Dat `AcceptUnsignedPayloads=false`
3. Gui request co header `X-Hub-Signature-256`

Neu gui payload khong dung chu ky, API phai tra `401 Unauthorized`.

## 14. Lenh dung he thong

Dung `webhook-service` local trong terminal dang chay `dotnet run` bang `Ctrl + C`.

Sau do dung Docker stack trong thu muc `BaiKT4`:

```powershell
docker compose down
```
