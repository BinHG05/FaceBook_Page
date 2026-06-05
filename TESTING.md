# Hướng dẫn Kiểm thử Hệ thống (Testing Guide)

Tài liệu này hướng dẫn chi tiết cách kiểm thử các luồng nghiệp vụ trong dự án, bao gồm chạy giả lập ngoại tuyến (Offline Mode) và tích hợp các kịch bản lỗi để kiểm tra luồng **Retry** và **Dead Letter Queue (DLQ)**.

---

## 🛠️ Chuẩn bị Môi trường

Đảm bảo bạn đã khởi chạy hệ thống giám sát và các dịch vụ hỗ trợ bằng Docker:
```powershell
docker compose up --build -d
```
Kiểm tra danh sách các dịch vụ đang chạy bằng lệnh:
```powershell
docker compose ps
```

---

## 1. Kiểm thử Luồng 1: Nhận Comment & Tự động Phản hồi (Auto-Reply)

Để thực hiện kiểm thử nhanh mà không cần liên kết trực tiếp với Facebook Developer Portal, chúng ta sẽ giả lập các sự kiện gửi từ Facebook Webhook bằng công cụ `cURL` thông qua các mẫu dữ liệu trong thư mục `samples/`.

### Kịch bản 1.1: Gửi bình luận hỏi giá (Tự động trả lời qua AI)
Sự kiện [positive-event.json](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/BaiKT4/samples/positive-event.json) chứa bình luận hỏi giá sản phẩm của khách hàng:
```powershell
curl -X POST http://localhost:3001/webhook `
  -H "Content-Type: application/json" `
  --data-binary "@samples/positive-event.json"
```
*   **Kết quả mong đợi**:
    *   Webhook Service trả về phản hồi: `{"received":true,"publishedCount":1}`.
    *   Mở **Kafka UI** (`http://localhost:8085`), xem trong topic `raw_events` xuất hiện sự kiện vừa gửi.
    *   Core Service sẽ gọi Gemini AI phân tích thái độ tích cực (`tich_cuc`) và ý định hỏi giá (`hoi_gia`).
    *   Core Service đẩy lệnh phản hồi sang topic `reply_commands`.
    *   Backend API tiêu thụ lệnh và in ra log xử lý thành công (chế độ giả lập `SimulateMode=true`).

### Kịch bản 1.2: Gửi bình luận chứa từ cấm hoặc liên kết lừa đảo (Spam/Malicious)
Sự kiện [spam-event.json](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/BaiKT4/samples/spam-event.json) chứa liên kết độc hại hoặc từ khóa cấm:
```powershell
curl -X POST http://localhost:3001/webhook `
  -H "Content-Type: application/json" `
  --data-binary "@samples/spam-event.json"
```
*   **Kết quả mong đợi**:
    *   Core Service phát hiện nội dung có liên kết spam (`http://...`).
    *   Hành động đưa ra: ẩn bình luận (`hide_comment`) và đưa sự kiện này vào hàng đợi kiểm duyệt thủ công `ManualReviewQueue` trong cơ sở dữ liệu SQL Server.
    *   Kiểm tra log của Core Service bằng lệnh:
        ```powershell
        docker compose logs core-service
        ```

---

## 2. Kiểm thử Luồng 2: Luồng lỗi gửi tin, Thử lại (Retry) & Hòm thư chết (DLQ)

Luồng này kiểm thử khả năng phục hồi của hệ thống khi quá trình gửi phản hồi sang Facebook Graph API bị lỗi.

### Kịch bản 2.1: Lỗi có thể thử lại (Retryable Error) - Lỗi mất kết nối mạng
Chúng ta sẽ giả lập lỗi kết nối bằng cách chỉnh cấu hình Base URL của API Facebook sang một địa chỉ không tồn tại (lỗi phân giải DNS/timeout).

1.  Mở file [docker-compose.yml](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/BaiKT4/docker-compose.yml) tại cấu hình của dịch vụ `backend-api` (dòng 173-200).
2.  Thay đổi các biến cấu hình để tắt chế độ giả lập (`SimulateMode = false`) và trỏ URL sang một tên miền lỗi:
    ```yaml
    FacebookGraph__SimulateMode: "false"
    FacebookGraph__BaseUrl: "https://invalid-domain-facebook-api.xyz"
    FacebookGraph__PageAccessToken: "EAA..."
    ```
3.  Khởi động lại Backend API để áp dụng cấu hình mới:
    ```powershell
    docker compose up -d backend-api
    ```
4.  Gửi lại một yêu cầu cURL hợp lệ để kích hoạt luồng tự động trả lời:
    ```powershell
    curl -X POST http://localhost:3001/webhook -H "Content-Type: application/json" --data-binary "@samples/positive-event.json"
    ```
5.  **Quan sát tiến trình chạy**:
    *   **Lần gửi thứ nhất**: [CommandDispatchWorker](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/BaiKT4/BaiKT4.BackendApi/Services/CommandDispatchWorker.cs) ở Backend API nhận lệnh và cố gắng gọi HTTP Client sang tên miền lỗi. Yêu cầu thất bại ném ra `HttpRequestException`.
    *   Do không có mã trạng thái HTTP (không kết nối được), hàm `IsRetryable` đánh giá là **Retryable = true**. Backend API đẩy sự kiện lỗi vào topic `send_failed`.
    *   **Retry Service** tiêu thụ lỗi từ `send_failed`, ghi nhận `RetryCount = 0` (lần đầu tiên lỗi). Nó tính toán thời gian trễ $2^0 = 1$ giây, thực hiện `await Task.Delay(1000)` rồi đẩy lại lệnh với `RetryCount = 1` vào topic **`send_retry`**.
    *   **Lần thử lại thứ nhất**: Backend API nhận lệnh từ `send_retry`, gửi lại và tiếp tục thất bại. Lỗi đẩy sang `send_failed`.
    *   **Retry Service** tiêu thụ tiếp lỗi, trì hoãn $2^1 = 2$ giây, đẩy lại lệnh với `RetryCount = 2` vào `send_retry`.
    *   **Lần thử lại thứ hai**: Tiếp tục thất bại, trì hoãn $2^2 = 4$ giây, đẩy lại lệnh với `RetryCount = 3` vào `send_retry`.
    *   **Lần thử lại cuối cùng**: Tiếp tục thất bại. Lần này `nextRetryCount = 4` vượt quá ngưỡng `MaxRetryCount = 3` cấu hình trong hệ thống.
    *   **Chuyển vào DLQ**: Retry Service không gửi lại nữa mà đóng gói lệnh lỗi thành `DeadLetterEvent` và đẩy vào topic **`dead_letter`** (Hòm thư chết).

### Kịch bản 2.2: Lỗi KHÔNG THỂ thử lại (Non-Retryable Error) - Sai token (401 Unauthorized)
Nếu mã lỗi phản hồi từ Facebook Graph API xác định chắc chắn rằng lệnh gửi lại sẽ tiếp tục thất bại (như sai token truy cập), hệ thống sẽ chuyển thẳng tin nhắn vào DLQ mà không thử lại để tránh lãng phí tài nguyên.

1.  Cấu hình lại dịch vụ `backend-api` trong [docker-compose.yml](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/BaiKT4/docker-compose.yml):
    ```yaml
    FacebookGraph__SimulateMode: "false"
    FacebookGraph__BaseUrl: "https://graph.facebook.com/v25.0"
    FacebookGraph__PageAccessToken: "SAI_ACCESS_TOKEN_AO"
    ```
2.  Khởi động lại Backend API:
    ```powershell
    docker compose up -d backend-api
    ```
3.  Gửi yêu cầu cURL kích hoạt trả lời:
    ```powershell
    curl -X POST http://localhost:3001/webhook -H "Content-Type: application/json" --data-binary "@samples/positive-event.json"
    ```
4.  **Quan sát tiến trình**:
    *   Backend API gọi lên Facebook Graph API và nhận về mã lỗi HTTP **401 Unauthorized**.
    *   Hàm `IsRetryable` đánh giá là **false**.
    *   Sự kiện lỗi được gửi sang topic `send_failed`.
    *   **Retry Service** tiêu thụ từ `send_failed`. Nhận thấy lỗi không thể thử lại (`Retryable = false`), nó bỏ qua bước chờ delay và chuyển thẳng thông tin lỗi sang topic **`dead_letter`** (DLQ).

---

## 3. Kiểm thử Cảnh báo Hệ thống (Prometheus & Alertmanager Alert)

Sau khi kiểm thử xong luồng lỗi ở **Kịch bản 2.1** hoặc **Kịch bản 2.2** (khi đã có ít nhất một tin nhắn lỗi rơi vào topic `dead_letter`):

1.  Mở giao diện Prometheus tại địa chỉ: `http://localhost:9090`.
2.  Nhập tìm kiếm metric: `kafka_topic_partition_current_offset{topic="dead_letter"}` để kiểm tra xem Prometheus đã ghi nhận chỉ số offset của topic DLQ tăng lên chưa.
3.  Chuyển sang tab **Alerts** trên menu thanh công cụ của Prometheus. Bạn sẽ thấy trạng thái cảnh báo **`DeadLetterTopicReceivedMessages`**:
    *   Màu vàng (`PENDING`): Đang chờ điều kiện thời gian đánh giá (luật quy định chờ đủ 15 giây).
    *   Màu đỏ (`FIRING`): Cảnh báo đã được kích hoạt chính thức và gửi sang Alertmanager.
4.  Mở giao diện Alertmanager tại địa chỉ: `http://localhost:9093`. Bạn sẽ thấy cảnh báo đang được nhóm và gửi đi.
5.  Kiểm tra logs của dịch vụ webhook nhận cảnh báo lỗi để xác minh luồng nhận cảnh báo hoạt động thành công:
    ```powershell
    docker compose logs alert-webhook
    ```
    *   **Dòng log mong đợi**:
        ```text
        ALERT RECEIVED {"receiver": "dead-letter-webhook", "status": "firing", "alerts": [...], ...}
        ```
