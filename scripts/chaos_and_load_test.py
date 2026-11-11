import concurrent.futures
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://127.0.0.1:3001/webhook")

# Sample scenarios
SAMPLE_POSITIVE = [
    "Shop đóng gói cẩn thận, hàng nhận được rất ưng ý!",
    "Dịch vụ hỗ trợ rất nhiệt tình và chu đáo, 10 điểm!",
    "Chất vải mềm mịn đúng như mô tả, mình sẽ ủng hộ tiếp.",
    "Giao hàng siêu nhanh luôn shop ơi, cảm ơn shop nhiều nhé."
]

SAMPLE_INQUIRY = [
    "Mẫu này còn size L màu đen không shop?",
    "Cho mình hỏi sản phẩm này giá bao nhiêu vậy ạ?",
    "Phí ship về quận Cầu Giấy Hà Nội là bao nhiêu shop?",
    "Có được kiểm tra hàng trước khi thanh toán không shop?"
]

SAMPLE_NEGATIVE = [
    "Giao sai mẫu rồi shop ơi, làm ăn kiểu gì vậy?",
    "Nhắn tin từ sáng đến giờ không thấy ai phản hồi, dịch vụ tệ quá.",
    "Hàng bị trầy xước và bẩn, đề nghị shop hỗ trợ đổi trả ngay!",
    "Chờ hàng 2 tuần mới tới, quá thất vọng với trải nghiệm lần này."
]

SAMPLE_SPAM = [
    "Nhận quà miễn phí 500k tại http://scam-link.vip",
    "Kiếm tiền online tại nhà không cần vốn ib zalo 0987654321",
    "Khuyến mãi sốc chỉ hôm nay click http://bit.ly/nhanqua"
]

def make_payload(message, user_id=None, comment_id=None):
    if user_id is None:
        user_id = f"user_{random.randint(1000, 9999)}"
    if comment_id is None:
        comment_id = f"cmt_{int(time.time()*1000)}_{random.randint(10, 99)}"

    return {
        "object": "page",
        "entry": [
            {
                "id": "1029208276949641",
                "time": int(time.time()),
                "changes": [
                    {
                        "field": "feed",
                        "value": {
                            "item": "comment",
                            "verb": "add",
                            "comment_id": comment_id,
                            "post_id": "1029208276949641_post_999",
                            "message": message,
                            "created_time": time.strftime("%Y-%m-%dT%H:%M:%S+07:00"),
                            "from": {
                                "id": user_id,
                                "name": f"User {user_id}"
                            }
                        }
                    }
                ]
            }
        ]
    }

def send_single_webhook(payload):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(WEBHOOK_URL, data=data, headers={"Content-Type": "application/json"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            latency = (time.time() - t0) * 1000
            return response.status, latency, None
    except urllib.error.HTTPError as e:
        latency = (time.time() - t0) * 1000
        return e.code, latency, str(e)
    except Exception as e:
        latency = (time.time() - t0) * 1000
        return 0, latency, str(e)

def run_mixed_traffic(count=20, delay_sec=0.5):
    print(f"\n🚀 [SCENARIO 1] Sending {count} mixed events with {delay_sec}s delay...")
    sent = 0
    for i in range(count):
        cat = random.choices(["pos", "inq", "neg", "spam"], weights=[40, 30, 20, 10])[0]
        if cat == "pos":
            msg = random.choice(SAMPLE_POSITIVE)
        elif cat == "inq":
            msg = random.choice(SAMPLE_INQUIRY)
        elif cat == "neg":
            msg = random.choice(SAMPLE_NEGATIVE)
        else:
            msg = random.choice(SAMPLE_SPAM)

        payload = make_payload(msg)
        status, lat, err = send_single_webhook(payload)
        if status == 200:
            sent += 1
            print(f"  [{i+1}/{count}] ✅ Sent ({cat.upper()}): '{msg[:35]}...' ({lat:.1f}ms)")
        else:
            print(f"  [{i+1}/{count}] ❌ Error: {err}")
        time.sleep(delay_sec)
    print(f"🎉 Completed sending {sent}/{count} events.\n")

def run_burst_load(total_requests=500, concurrency=25):
    print(f"\n⚡ [SCENARIO 2] BURST LOAD TEST: Sending {total_requests} events (Concurrency: {concurrency})...")
    print("   👉 This will demonstrate Backpressure & Consumer Lag spike in Prometheus / Kafka UI / Streamlit!")
    
    payloads = []
    for i in range(total_requests):
        msg = random.choice(SAMPLE_POSITIVE + SAMPLE_INQUIRY + SAMPLE_NEGATIVE)
        payloads.append(make_payload(msg, comment_id=f"burst_{i}_{int(time.time())}"))

    t_start = time.time()
    success_count = 0
    latencies = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(send_single_webhook, p) for p in payloads]
        for f in concurrent.futures.as_completed(futures):
            status, lat, err = f.result()
            latencies.append(lat)
            if status == 200:
                success_count += 1

    total_time = time.time() - t_start
    throughput = success_count / total_time if total_time > 0 else 0
    avg_lat = sum(latencies) / len(latencies) if latencies else 0

    print(f"\n🏁 BURST TEST SUMMARY:")
    print(f"   - Total Sent: {success_count}/{total_requests} (Success Rate: {success_count/total_requests*100:.1f}%)")
    print(f"   - Elapsed Time: {total_time:.2f} seconds")
    print(f"   - Ingestion Throughput: {throughput:.1f} requests/sec")
    print(f"   - Average Webhook Latency: {avg_lat:.1f} ms")
    print(f"\n👀 NOW CHECK:")
    print(f"   1. Streamlit Dashboard: http://localhost:8501 (Watch Comment Velocity & Anomaly Spike!)")
    print(f"   2. Kafka UI: http://localhost:8085 (Watch raw_events partition lag absorb the burst)")
    print(f"   3. Prometheus: http://localhost:9090 (Watch consumergroup lag metrics)\n")

def run_blacklist_test():
    print("\n🚫 [SCENARIO 3] BLACKLIST AUTOMATION TEST:")
    spammer_id = f"spammer_{random.randint(100, 999)}"
    print(f"   Simulating user '{spammer_id}' posting 3 spam messages sequentially...")

    for i in range(3):
        payload = make_payload(f"Quà tặng miễn phí click http://scam.me/ref={i}", user_id=spammer_id)
        status, _, _ = send_single_webhook(payload)
        print(f"   - Sent spam #{i+1} (Status: {status})")
        time.sleep(1)

    print(f"✅ User '{spammer_id}' has been recorded. CoreService evaluates repeat offenses in 24h -> Triggers 'blacklist' action!\n")

if __name__ == "__main__":
    print("=" * 65)
    print("  🚀 FACEBOOK STREAMING PLATFORM - CHAOS & LOAD TEST SUITE")
    print("=" * 65)
    print("Target Webhook URL:", WEBHOOK_URL)
    print("1. Send Mixed Realistic Traffic (20 events, 0.5s interval)")
    print("2. Run Burst Load Test (500 events concurrently - Test Consumer Lag)")
    print("3. Test Blacklist Rule Engine (3 spam events from same user)")
    print("4. Quick Health Check (Send 1 event)")
    print("=" * 65)

    choice = input("Enter choice [1-4] (default 1): ").strip() or "1"

    if choice == "1":
        run_mixed_traffic(count=20, delay_sec=0.5)
    elif choice == "2":
        run_burst_load(total_requests=500, concurrency=30)
    elif choice == "3":
        run_blacklist_test()
    elif choice == "4":
        run_mixed_traffic(count=1, delay_sec=0)
    else:
        print("Invalid choice.")

