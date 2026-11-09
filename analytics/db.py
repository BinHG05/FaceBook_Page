import os
import sqlite3
import time
from datetime import datetime, timezone

DB_PATH = os.getenv("METRICS_DB_PATH", os.path.join(os.path.dirname(__file__), "metrics.db"))

def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE,
                page_id TEXT,
                user_id TEXT,
                message_text TEXT,
                received_at REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reply_commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                command_id TEXT UNIQUE,
                event_id TEXT,
                action_type TEXT,
                reply_message TEXT,
                sentiment TEXT,
                intent TEXT,
                retry_count INTEGER DEFAULT 0,
                processed_at REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dead_letters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dead_letter_id TEXT UNIQUE,
                command_id TEXT,
                reason TEXT,
                retry_count INTEGER,
                occurred_at REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS windowed_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL,
                iso_time TEXT,
                window_seconds INTEGER,
                event_count INTEGER,
                velocity_per_min REAL,
                positive_count INTEGER,
                negative_count INTEGER,
                neutral_count INTEGER,
                spam_count INTEGER,
                dlq_count INTEGER,
                is_spike INTEGER DEFAULT 0
            )
        """)
        conn.commit()

def record_raw_event(event_id, page_id, user_id, message_text, received_at=None):
    if received_at is None:
        received_at = time.time()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO raw_events (event_id, page_id, user_id, message_text, received_at)
            VALUES (?, ?, ?, ?, ?)
        """, (event_id, page_id, user_id, message_text, received_at))
        conn.commit()

def record_reply_command(command_id, event_id, action_type, reply_message, sentiment="trung_tinh", intent="khac", retry_count=0, processed_at=None):
    if processed_at is None:
        processed_at = time.time()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO reply_commands (command_id, event_id, action_type, reply_message, sentiment, intent, retry_count, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (command_id, event_id, action_type, reply_message, sentiment, intent, retry_count, processed_at))
        conn.commit()

def record_dead_letter(dead_letter_id, command_id, reason, retry_count, occurred_at=None):
    if occurred_at is None:
        occurred_at = time.time()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO dead_letters (dead_letter_id, command_id, reason, retry_count, occurred_at)
            VALUES (?, ?, ?, ?, ?)
        """, (dead_letter_id, command_id, reason, retry_count, occurred_at))
        conn.commit()

def compute_window_snapshot(window_seconds=60):
    now = time.time()
    window_start = now - window_seconds
    iso_str = datetime.now(timezone.utc).strftime("%H:%M:%S")

    with get_connection() as conn:
        cursor = conn.cursor()

        # Count events in this window
        cursor.execute("SELECT COUNT(*) FROM raw_events WHERE received_at >= ?", (window_start,))
        event_count = cursor.fetchone()[0]
        velocity_per_min = (event_count / (window_seconds / 60.0))

        # Sentiment breakdown from reply_commands
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN sentiment = 'tich_cuc' THEN 1 ELSE 0 END),
                SUM(CASE WHEN sentiment = 'tieu_cuc' THEN 1 ELSE 0 END),
                SUM(CASE WHEN sentiment = 'trung_tinh' THEN 1 ELSE 0 END),
                SUM(CASE WHEN action_type = 'hide_comment' OR action_type = 'blacklist' THEN 1 ELSE 0 END)
            FROM reply_commands
            WHERE processed_at >= ?
        """, (window_start,))
        row = cursor.fetchone()
        pos_count = row[0] or 0
        neg_count = row[1] or 0
        neu_count = row[2] or 0
        spam_count = row[3] or 0

        # DLQ count in this window
        cursor.execute("SELECT COUNT(*) FROM dead_letters WHERE occurred_at >= ?", (window_start,))
        dlq_count = cursor.fetchone()[0]

        # Spike detection: compare velocity with previous 5 windows average
        cursor.execute("""
            SELECT AVG(velocity_per_min) FROM (
                SELECT velocity_per_min FROM windowed_metrics 
                ORDER BY id DESC LIMIT 5
            )
        """)
        avg_row = cursor.fetchone()
        historical_avg = avg_row[0] if (avg_row and avg_row[0] is not None) else 0

        # Anomaly threshold: 2x historical average and at least 15 events/min
        is_spike = 1 if (velocity_per_min >= 15 and (historical_avg == 0 or velocity_per_min > historical_avg * 2.0)) else 0

        cursor.execute("""
            INSERT INTO windowed_metrics (timestamp, iso_time, window_seconds, event_count, velocity_per_min, positive_count, negative_count, neutral_count, spam_count, dlq_count, is_spike)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (now, iso_str, window_seconds, event_count, velocity_per_min, pos_count, neg_count, neu_count, spam_count, dlq_count, is_spike))
        conn.commit()

def get_kpis():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw_events")
        total_events = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM reply_commands")
        total_commands = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM dead_letters")
        total_dlq = cursor.fetchone()[0]

        # Sentiment counts
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN sentiment = 'tich_cuc' THEN 1 ELSE 0 END),
                SUM(CASE WHEN sentiment = 'tieu_cuc' THEN 1 ELSE 0 END),
                SUM(CASE WHEN sentiment = 'trung_tinh' THEN 1 ELSE 0 END),
                SUM(CASE WHEN action_type = 'hide_comment' OR action_type = 'blacklist' THEN 1 ELSE 0 END)
            FROM reply_commands
        """)
        row = cursor.fetchone()
        pos = row[0] or 0
        neg = row[1] or 0
        neu = row[2] or 0
        spam = row[3] or 0

        # Current velocity & Peak velocity
        now = time.time()
        cursor.execute("SELECT COUNT(*) FROM raw_events WHERE received_at >= ?", (now - 60,))
        current_rpm = cursor.fetchone()[0]

        cursor.execute("SELECT MAX(velocity_per_min) FROM windowed_metrics")
        max_row = cursor.fetchone()
        peak_rpm = max_row[0] if (max_row and max_row[0] is not None) else 0

        failure_rate = (total_dlq / total_commands * 100.0) if total_commands > 0 else 0.0

        return {
            "total_events": total_events,
            "total_commands": total_commands,
            "total_dlq": total_dlq,
            "positive_count": pos,
            "negative_count": neg,
            "neutral_count": neu,
            "spam_count": spam,
            "current_rpm": current_rpm,
            "peak_rpm": int(peak_rpm),
            "failure_rate": round(failure_rate, 1)
        }

def get_velocity_history(limit=50):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT iso_time, velocity_per_min, event_count, is_spike
            FROM windowed_metrics
            ORDER BY id DESC LIMIT ?
        """, (limit,))
        rows = cursor.fetchall()
        return [dict(row) for row in reversed(rows)]

def get_recent_dlq(limit=10):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT dead_letter_id, command_id, reason, retry_count, datetime(occurred_at, 'unixepoch') as formatted_time
            FROM dead_letters
            ORDER BY id DESC LIMIT ?
        """, (limit,))
        return [dict(row) for row in cursor.fetchall()]

def get_recent_moderation_logs(limit=15):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT command_id, event_id, action_type, sentiment, intent, reply_message, datetime(processed_at, 'unixepoch') as formatted_time
            FROM reply_commands
            ORDER BY id DESC LIMIT ?
        """, (limit,))
        return [dict(row) for row in cursor.fetchall()]

