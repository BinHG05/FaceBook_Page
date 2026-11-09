# Event-Driven Streaming Data Platform | Real-Time Analytics

Hệ thống Streaming Data Platform hướng sự kiện (Event-Driven Architecture) kiến trúc dòng kép (**Dual-Stream**): Vừa thực thi kiểm duyệt nội dung, tự động phản hồi Facebook qua AI (Operational Stream), vừa phân tích, tổng hợp dữ liệu theo cửa sổ thời gian (Windowing Stream Analytics) và trực quan hóa thời gian thực trên Streamlit Dashboard.

---

## 🏗️ Kiến trúc Hệ thống Dòng Kép (Dual-Stream Architecture)

```mermaid
flowchart TD
    FB["Facebook Webhooks / Traffic Simulator"] -->|1. Webhook POST| WH["Webhook Service :3001"]
    
    subgraph Governance["Data Governance & Contracts"]
        SR["Confluent Schema Registry :8081<br/>JSON Schema Contracts"]
        WH <-->|Validate & Register Schema| SR
    end

    WH -->|2. raw_events| Kafka[Kafka Broker]

    subgraph OperationalStream["1. Operational Stream (C# .NET 9 Microservices)"]
        Kafka -->|3. Consume raw_events| Core["Core Service :3002<br/>Gemini AI + Rule Engine"]
        Core -->|4. Audit log| SQL[("SQL Server")]
        Core -->|5. reply_commands| Kafka
        Kafka -->|6. Consume reply_commands & send_retry| Backend["Backend API :3000<br/>Idempotency + Circuit Breaker"]
        Backend -->|7. Dispatch Reply| FB_API["Facebook Graph API"]
        Backend -->|8. If Fail -> send_failed| Kafka
        Kafka -->|9. Consume send_failed| Retry["Retry Service :3003<br/>Exponential Backoff"]
        Retry -->|10. Retry <= 3 -> send_retry| Kafka
        Retry -->|11. Exceeded -> dead_letter| Kafka
    end

    subgraph AnalyticalStream["2. Analytical Stream (Python + Streamlit)"]
        Kafka -.->|Consume raw_events, reply_commands, dead_letter| Analytics["Analytics Worker<br/>(1m Tumbling & 5m Sliding Window)"]
        Analytics -->|Emit Aggregations| MDB[("SQLite Time-Series Metrics")]
        MDB -->|Live Refresh (2s)| Dash["Streamlit Live Dashboard :8501<br/>- Sentiment Trends<br/>- Comment Velocity & Spike Alert<br/>- Failure & DLQ Rates"]
    end

    subgraph SRE["3. Real-Time Observability & Monitoring"]
        Kafka -->|Metrics Scrape| Exporter["Kafka Exporter :9308"]
        Exporter -->|Scraped by| Prom["Prometheus :9090"]
        Prom -->|Trigger Alerts| AM["Alertmanager :9093"]
        AM -->|Webhook alert| Alert["Alert Webhook :18080"]
    end
```

---

## 🗂️ Cấu trúc Dịch vụ & Thư mục

*   **[FbStream.Contracts](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/FbStream/FbStream.Contracts)**: Định nghĩa Model C# dùng chung giữa các microservices.
*   **[contracts/schemas/](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/FbStream/contracts/schemas)**: JSON Schemas (Data Contracts) đăng ký lên Schema Registry.
*   **[FbStream.WebhookService](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/FbStream/FbStream.WebhookService)**: Nhận webhook, verify chữ ký HMAC-SHA256, chuẩn hóa payload và publish vào `raw_events`.
*   **[FbStream.CoreService](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/FbStream/FbStream.CoreService)**: Kiểm tra spam/blacklist, gọi Gemini AI phân tích intent/sentiment, thực thi rule engine và publish vào `reply_commands`.
*   **[FbStream.BackendApi](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/FbStream/FbStream.BackendApi)**: API Proxy, kiểm tra Idempotency Key, Circuit Breaker, gọi Facebook Graph API và bắt lỗi sang `send_failed`.
*   **[FbStream.RetryService](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/FbStream/FbStream.RetryService)**: Exponential Backoff Retry ($1s \times 2^{\text{retry\_count}}$) và phân loại lỗi đẩy vào `dead_letter` (DLQ).
*   **[analytics/](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/FbStream/analytics)**: Dịch vụ Python Stream Analytics + Streamlit Dashboard thời gian thực (Cổng 8501).
*   **[monitoring/](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/FbStream/monitoring)**: Prometheus, Alertmanager, Kafka Exporter giám sát Consumer Lag, DLQ và Traffic Spike.
*   **[scripts/](file:///d:/N%C4%83m%203%20-%20K%E1%BB%B3%202/API/FbStream/scripts)**: Công cụ đăng ký Schema Contract và bộ test tải đột biến (Burst Load 500+ requests).

---

## 📨 Kafka Topics & Data Contracts

| Tên Topic | Producers | Consumers | Schema Contract | Mục đích / Nghiệp vụ |
| :--- | :--- | :--- | :--- | :--- |
| **`raw_events`** | `WebhookService` | `CoreService`, `AnalyticsService` | `normalized_webhook_event.json` | Sự kiện webhook chuẩn hóa từ Facebook. |
| **`reply_commands`** | `CoreService` | `BackendApi`, `AnalyticsService` | `reply_command.json` | Lệnh phản hồi tự động / ẩn comment. |
| **`send_failed`** | `BackendApi` | `RetryService` | `SendFailedEvent` | Lệnh gửi thất bại cần đánh giá thử lại. |
| **`send_retry`** | `RetryService` | `BackendApi` | `ReplyCommand` | Lệnh lỗi được lên lịch gửi lại sau thời gian chờ. |
| **`dead_letter`** | `RetryService` | `AnalyticsService`, `Prometheus` | `DeadLetterEvent` | Hòm thư chết (DLQ) chứa lệnh lỗi vĩnh viễn hoặc quá số lần thử. |

---

## ⚡ Hướng dẫn Chạy Hệ thống

### 1. Chuẩn bị biến môi trường
Tạo file `.env` từ `.env.example`:
```powershell
Copy-Item .env.example .env
```
Điền các biến API Token cần thiết trong file `.env`:
```env
GEMINI_API_KEY=your_gemini_api_key
FACEBOOK_PAGE_ACCESS_TOKEN=your_token_or_leave_dummy
ADMIN_API_KEY=admin-secret-key-123
```

### 2. Khởi chạy toàn bộ hệ thống bằng Docker Compose
```powershell
docker compose up --build -d
```

### 3. Đăng ký Data Contracts lên Schema Registry
```powershell
python scripts/register_schemas.py
```

### 4. Danh sách các Cổng Dịch vụ (Web UI)
*   🌐 **Streamlit Real-Time Dashboard**: [`http://localhost:8501`](http://localhost:8501)
*   📊 **Kafka UI (Quản lý Broker & Topics)**: [`http://localhost:8085`](http://localhost:8085)
*   📈 **Prometheus Alerts & Metrics**: [`http://localhost:9090`](http://localhost:9090)
*   🚨 **Alertmanager Incident Board**: [`http://localhost:9093`](http://localhost:9093)
*   🔌 **Backend API Proxy**: [`http://localhost:3000`](http://localhost:3000)
*   📥 **Facebook Webhook Endpoint**: `POST http://localhost:3001/webhook`
*   📜 **Confluent Schema Registry**: [`http://localhost:8081`](http://localhost:8081)

---

## 🧪 Kịch bản Kiểm thử & Demo Chuyên sâu

Chạy bộ công cụ kiểm thử tải & lỗi:
```powershell
python scripts/chaos_and_load_test.py
```
*   **Kịch bản 1 (Traffic thực tế):** Bắn 20 bình luận ngẫu nhiên để quan sát phân loại tích cực, tiêu cực, hỏi giá và ẩn comment spam trên Streamlit.
*   **Kịch bản 2 (Burst Load 500 requests - Backpressure Test):** Bắn đồng thời 500 request trong vài giây. Quan sát Consumer Lag tăng vọt trên Kafka UI & Prometheus, đồ thị Comment Velocity trên Streamlit bật cảnh báo Anomaly Spike, hệ thống hấp thụ tải mượt mà không crash.
*   **Kịch bản 3 (Blacklist Automation):** Bắn 3 bình luận spam liên tiếp từ 1 user để kích hoạt rule Blacklist nội bộ trong 24 giờ.
