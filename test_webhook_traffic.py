import urllib.request
import urllib.error
import json
import time
import random

# Cấu hình địa chỉ Webhook Service đang chạy
WEBHOOK_URL = "http://localhost:3001/webhook"

# Danh sách các kịch bản bình luận để giả lập thực tế
SCENARIOS = [
    # 1. User bình thường khen ngợi
    {"user_id": "U101", "name": "Nguyễn Văn A", "message": "Shop đóng gói cẩn thận, sản phẩm rất tuyệt vời!"},
    {"user_id": "U102", "name": "Trần Thị B", "message": "10 điểm không có nhưng, sẽ ủng hộ tiếp."},
    
    # 2. User hỏi giá/hỗ trợ
    {"user_id": "U103", "name": "Lê Văn C", "message": "Mẫu này còn size M không shop ơi? Giá bao nhiêu vậy?"},
    {"user_id": "U104", "name": "Phạm Thị D", "message": "Cho mình xin phí ship về Hà Nội nhé."},
    
    # 3. User phẫn nộ/khiếu nại (Cần AI phát hiện sentiment tiêu cực)
    {"user_id": "U105", "name": "Hoàng Văn E", "message": "Giao sai hàng rồi shop ơi, làm ăn kiểu gì vậy? Đề nghị hoàn tiền!"},
    {"user_id": "U106", "name": "Đinh Thị F", "message": "Nhắn tin từ hôm qua mà không ai trả lời, dịch vụ tệ quá."},
    
    # 4. User Spam thô (Chứa link độc hại)
    {"user_id": "U999", "name": "Bot Spam", "message": "Nhận ngay 500k miễn phí khi click vào http://scam-link.com"},
    
    # 5. User Spam lặp lại (Giả lập spam 3 lần để test rule Blacklist)
    {"user_id": "U888", "name": "Spammer 1", "message": "Mua hàng giá rẻ tại fb.com/shoprac"},
    {"user_id": "U888", "name": "Spammer 1", "message": "Mua hàng giá rẻ tại fb.com/shoprac"},
    {"user_id": "U888", "name": "Spammer 1", "message": "Mua hàng giá rẻ tại fb.com/shoprac"},
]

def create_payload(scenario, comment_id):
    """Tạo JSON payload giống hệt định dạng Facebook Webhook"""
    return {
        "object": "page",
        "entry": [
            {
                "id": "123456789", # ID của Fanpage
                "time": int(time.time()),
                "changes": [
                    {
                        "field": "feed",
                        "value": {
                            "item": "comment",
                            "verb": "add",
                            "comment_id": f"comment_{comment_id}",
                            "post_id": "123456789_111111",
                            "message": scenario["message"],
                            "created_time": time.strftime("%Y-%m-%dT%H:%M:%S+07:00"),
                            "from": {
                                "id": scenario["user_id"],
                                "name": scenario["name"]
                            }
                        }
                    }
                ]
            }
        ]
    }

def send_request(payload):
    """Gửi POST request tới Webhook"""
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(WEBHOOK_URL, data=data, headers={'Content-Type': 'application/json'})
    try:
        response = urllib.request.urlopen(req)
        print(f"✅ Đã gửi thành công: {payload['entry'][0]['changes'][0]['value']['message'][:30]}...")
    except urllib.error.URLError as e:
        print(f"❌ Lỗi gửi request: {e.reason}. (Bạn đã bật Webhook Service ở cổng 3001 chưa?)")

if __name__ == "__main__":
    print(f"🚀 Bắt đầu bắn {len(SCENARIOS)} tin nhắn test vào {WEBHOOK_URL}...")
    
    # Trộn ngẫu nhiên danh sách (trừ 3 cái cuối là spam lặp lại thì giữ nguyên để test)
    normal_messages = SCENARIOS[:-3]
    spam_messages = SCENARIOS[-3:]
    random.shuffle(normal_messages)
    
    all_test_cases = normal_messages + spam_messages
    
    for i, scenario in enumerate(all_test_cases):
        payload = create_payload(scenario, i)
        send_request(payload)
        # Nghỉ 0.5 giây giữa các lần bắn
        time.sleep(0.5)
        
    print("\n🎉 Hoàn tất! Hãy mở Console của WebhookService (hoặc SSMS) để xem kết quả phân tích từ AI nhé.")
