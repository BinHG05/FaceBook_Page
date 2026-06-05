# Huong dan dang ky Facebook Webhook theo dung giao dien trong anh

File nay viet lai theo giao dien Meta Developer tieng Viet ma ban dang mo, khong dung ten menu cu.

## Muc tieu

Sau khi lam xong, Facebook se goi:

- `GET /webhook` de xac minh callback URL
- `POST /webhook` de gui su kien ve he thong cua ban

Backend cua bai da san sang voi:

- Callback path: `/webhook`
- Port local: `3001`
- Verify token mac dinh: `baitk4-dev-token`

Neu ban chay local, Facebook se khong goi duoc `localhost`, nen ban can 1 URL cong khai bang:

- `ngrok`, hoac
- `cloudflared`

Vi du URL cong khai:

- `https://abc123.ngrok-free.app/webhook`

## Ban dang o dung man hinh nao

Theo anh ban gui, hien tai ban dang o:

- menu ben trai: `Webhooks`
- trang con: `Bang dieu khien`

Day la dung khu vuc de cau hinh webhook.

## Cach thao tac dung theo giao dien trong anh

### Buoc 1. Vao dung khu vuc Webhooks

Tren thanh ben trai:

1. Bam `Webhooks`
2. Bam `Bang dieu khien`

Neu da vao dung man hinh giong anh, ben phai se hien tieu de `Bang dieu khien`.

### Buoc 2. Mo phan them truong hop su dung

O goc phai tren, bam nut:

- `Them truong hop su dung`

Neu ban da them roi thi bo qua buoc nay.

### Buoc 3. Chon truong hop su dung lien quan toi Trang

Trong giao dien cua ban, o giua man hinh co dong:

- `Tuy chinh truong hop su dung Quan ly moi thu tren Trang`

Ban bam vao dong nay.

Muc dich la de app co cau hinh lien quan toi `Trang/Page`, vi webhook comment thuong nam o phan Page.

### Buoc 4. Mo phan cau hinh Webhooks cho Trang

Sau khi bam vao truong hop su dung tren, Meta co the dua ban toi mot trang chi tiet khac. O do, ban tim cac muc co noi dung gan nhu:

- `Webhooks`
- `Cau hinh`
- `Trang`
- `Page`
- `Dang ky truong`

Ten co the thay doi nhe theo tai khoan, nhung y chinh la ban phai tim cho ra phan cau hinh webhook cho `Trang/Page`.

## Neu ban khong thay nut "Subscribe to this object"

Trong giao dien moi cua Meta, nhieu khi no khong hien dung cau chu cu nhu:

- `Subscribe to this object`

Ma se hien theo kieu:

- `Them truong hop su dung`
- `Tuy chinh truong hop su dung ...`
- `Thu nghiem truong hop su dung`
- `Dang ky truong`
- `Them URL goi lai`
- `Cau hinh Webhook`

Nen ban cu tim theo y nghia sau:

1. Chon `Trang/Page`
2. Them callback URL
3. Nhap verify token
4. Chon cac truong su kien can nghe

## Buoc 5. Nhap Callback URL va Verify Token

Khi giao dien hien form cau hinh webhook, ban nhap:

- `Callback URL`
- `Verify token`

Gia tri can dung:

### Callback URL

Neu ban dung ngrok:

```text
https://tenmien-cong-khai/webhook
```

Vi du:

```text
https://abc123.ngrok-free.app/webhook
```

Khong duoc de:

```text
http://localhost:3001/webhook
```

vi Facebook khong goi vao localhost duoc.

### Verify Token

Nhap:

```text
baitk4-dev-token
```

Hoac neu ban doi token trong file config thi nhap dung token moi.

File lien quan:

- [appsettings.Development.json](/d:/Năm 3 - Kỳ 2/API/BaiKT4/BaiKT4.WebhookService/appsettings.Development.json)

## Buoc 6. Bam luu va de Facebook xac minh

Khi bam luu, Facebook se goi request dang nay:

```text
GET /webhook?hub.mode=subscribe&hub.verify_token=...&hub.challenge=...
```

Backend cua bai se tu dong:

- kiem tra `verify_token`
- neu dung se tra lai `hub.challenge`

Neu thanh cong thi callback URL da duoc xac minh.

## Buoc 7. Chon truong su kien de nghe

Sau khi verify xong, ban tim phan:

- `Dang ky truong`
- `Truong`
- `Subscribed fields`

Voi de bai nay, ban uu tien tim va chon:

- `feed`

Neu giao dien co them cac truong lien quan tin nhan thi co the chon them:

- `messages`
- `messaging_postbacks`
- cac truong messaging neu giao dien hien ra

De bai dang nhan manh comment, nen quan trong nhat la:

- `feed`

Ly do:

- Comment moi tren bai viet cua Page thuong di qua nhom su kien `feed`

## Buoc 8. Test thu cong tren he thong cua ban

Sau khi cau hinh xong:

1. Chay he thong bang `docker compose up --build`
2. Tao comment moi tren bai viet cua Page
3. Facebook gui webhook ve backend
4. Backend normalize event
5. Event duoc day vao topic `raw_events`
6. Mo Kafka UI de xem message

## Chinh xac ban can tim gi tren giao dien cua ban

Tu anh ban gui, ban hay tim theo thu tu nay:

1. `Webhooks`
2. `Bang dieu khien`
3. `Them truong hop su dung`
4. `Quan ly moi thu tren Trang`
5. Phan cau hinh webhook cho `Trang`
6. O form cau hinh: nhap `Callback URL` va `Verify token`
7. O phan chon su kien: chon `feed`

## Gia tri can copy vao giao dien

### Verify token

```text
baitk4-dev-token
```

### Callback URL

Neu dung ngrok:

```text
https://<domain-ngrok>/webhook
```

Neu dung cloudflared:

```text
https://<domain-trycloudflare>/webhook
```

## Neu ban muon dung Facebook that thay vi test local

Hien tai project dang de:

- `FacebookWebhook:AppSecret = ""`
- `FacebookWebhook:AcceptUnsignedPayloads = true`

De lam chuan hon voi Facebook that, sua trong config:

- `AppSecret` = app secret cua Meta app
- `AcceptUnsignedPayloads` = `false`

Luc nay backend se bat buoc kiem tra header:

- `X-Hub-Signature-256`

## Truong hop ban van khong thay dung nut

Giao dien Meta thay doi rat thuong xuyen. Neu trong man hinh tiep theo ban khong thay cac ten minh ghi o tren, hay chup them:

1. Sau khi bam `Them truong hop su dung`
2. Sau khi bam `Quan ly moi thu tren Trang`
3. Man hinh co form nhap URL hoac man hinh chon field

Chi can them 1 anh nua o dung buoc do, minh se chi tiep cho ban theo tung nut tren giao dien cua ban.
