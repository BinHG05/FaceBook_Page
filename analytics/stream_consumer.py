import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (Consumer) %(message)s")
logger = logging.getLogger("StreamConsumer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "analytics-stream-processor-group")
TOPICS = ["raw_events", "reply_commands", "dead_letter"]

def process_raw_event(msg_val):
    try:
        data = json.loads(msg_val)
        event_id = data.get("eventId") or data.get("EventId")
        page_id = data.get("pageId") or data.get("PageId")
        actor = data.get("actor") or data.get("Actor") or {}
        user_id = actor.get("id") or actor.get("Id") or "unknown"
        message_text = data.get("messageText") or data.get("MessageText") or ""
        
        db.record_raw_event(event_id, page_id, user_id, message_text, time.time())
        logger.debug("Ingested raw event: %s", event_id)
    except Exception as e:
        logger.error("Error processing raw event: %s", e)

def process_reply_command(msg_val):
    try:
        data = json.loads(msg_val)
        command_id = data.get("commandId") or data.get("CommandId")
        event_id = data.get("eventId") or data.get("EventId")
        action_type = data.get("actionType") or data.get("ActionType") or "none"
        reply_message = data.get("replyMessage") or data.get("ReplyMessage")
        retry_count = data.get("retryCount") or data.get("RetryCount") or 0
        
        metadata = data.get("metadata") or data.get("Metadata") or {}
        sentiment = metadata.get("sentiment", "trung_tinh")
        intent = metadata.get("intent", "khac")

        # Heuristic inference if metadata doesn't contain it directly
        if action_type == "hide_comment" or action_type == "blacklist":
            sentiment = "spam"
        elif "Cam on" in str(reply_message):
            sentiment = "tich_cuc"
        elif "xin loi" in str(reply_message):
            sentiment = "tieu_cuc"
        elif "bang gia" in str(reply_message):
            intent = "hoi_gia"

        db.record_reply_command(command_id, event_id, action_type, reply_message, sentiment, intent, retry_count, time.time())
        logger.debug("Ingested reply command: %s (Action: %s)", command_id, action_type)
    except Exception as e:
        logger.error("Error processing reply command: %s", e)

def process_dead_letter(msg_val):
    try:
        data = json.loads(msg_val)
        dead_letter_id = data.get("deadLetterId") or data.get("DeadLetterId") or f"dlq_{int(time.time()*1000)}"
        command = data.get("command") or data.get("Command") or {}
        command_id = command.get("commandId") or command.get("CommandId") or "unknown"
        reason = data.get("reason") or data.get("Reason") or "unknown_error"
        retry_count = data.get("retryCount") or data.get("RetryCount") or 0

        db.record_dead_letter(dead_letter_id, command_id, reason, retry_count, time.time())
        logger.warning("🚨 INGESTED DEAD LETTER: %s (Command: %s, Reason: %s)", dead_letter_id, command_id, reason)
    except Exception as e:
        logger.error("Error processing dead letter: %s", e)

def run_window_aggregation_loop(interval_sec=5):
    """Background thread that computes sliding/tumbling snapshots into windowed_metrics table."""
    logger.info("Started Window Aggregation Timer (every %d seconds)", interval_sec)
    while True:
        try:
            db.compute_window_snapshot(window_seconds=60)
        except Exception as e:
            logger.error("Error in window aggregation: %s", e)
        time.sleep(interval_sec)

def start_kafka_consumer():
    db.init_db()

    # Launch window aggregation background worker
    agg_thread = threading.Thread(target=run_window_aggregation_loop, daemon=True)
    agg_thread.start()

    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    }

    logger.info("Connecting Kafka consumer to: %s, topics: %s", BOOTSTRAP_SERVERS, TOPICS)
    
    # Retry loop for Kafka connection on container startup
    consumer = None
    while consumer is None:
        try:
            consumer = Consumer(conf)
            consumer.subscribe(TOPICS)
            logger.info("✅ Consumer successfully connected and subscribed to: %s", TOPICS)
        except Exception as e:
            logger.warning("Waiting for Kafka Broker at %s (%s)... retrying in 3s", BOOTSTRAP_SERVERS, e)
            time.sleep(3)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error("Kafka error: %s", msg.error())
                    continue

            val_str = msg.value().decode("utf-8")
            topic = msg.topic()

            if topic == "raw_events":
                process_raw_event(val_str)
            elif topic == "reply_commands":
                process_reply_command(val_str)
            elif topic == "dead_letter":
                process_dead_letter(val_str)

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_kafka_consumer()

