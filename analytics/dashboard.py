import datetime
import os
import threading
import time
import altair as alt
import pandas as pd
import streamlit as st

import db

st.set_page_config(
    page_title="Facebook Real-Time Streaming Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize database
db.init_db()

# Auto-start Stream Consumer thread if not already running in this container
if "consumer_started" not in st.session_state:
    try:
        from stream_consumer import start_kafka_consumer
        consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
        consumer_thread.start()
        st.session_state["consumer_started"] = True
    except Exception as e:
        st.session_state["consumer_started"] = False
        st.sidebar.warning(f"Consumer init notice: {e}")

# Sidebar Controls
st.sidebar.title("⚡ Streaming Controls")
st.sidebar.markdown("**Dual-Stream Real-Time Analytics**")
auto_refresh = st.sidebar.checkbox("🔄 Live Stream Auto-Refresh", value=True)
refresh_rate = st.sidebar.slider("Refresh Interval (seconds)", min_value=1, max_value=10, value=2)

st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Architecture Info")
st.sidebar.info(
    """
    **Architecture:** Event-Driven Dual-Stream  
    **Broker:** Apache Kafka (Confluent 7.9.6)  
    **Topics Consumed:**  
    - `raw_events`  
    - `reply_commands`  
    - `dead_letter`  
    **Windowing:** 1m Tumbling, 5m Sliding  
    **Observability:** Prometheus + Alertmanager
    """
)

# Header Section
st.markdown("# 🚀 Facebook Webhook Streaming Analytics")
st.caption(f"Real-Time Stream Processing & Resilient Event Telemetry | Last Updated: {datetime.datetime.now().strftime('%H:%M:%S')}")

# Fetch KPI Data
kpis = db.get_kpis()

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Total Events Ingested", f"{kpis['total_events']:,}", delta=f"{kpis['current_rpm']} rpm")
with col2:
    st.metric("Current Velocity", f"{kpis['current_rpm']} evt/min", delta=f"Peak: {kpis.get('peak_rpm', 0)} rpm")
with col3:
    st.metric("Positive Sentiment", f"{kpis['positive_count']}")
with col4:
    st.metric("Negative Sentiment", f"{kpis['negative_count']}", delta_color="inverse")
with col5:
    dlq_delta = "🚨 Alert" if kpis['total_dlq'] > 0 else "Normal"
    st.metric("Dead Letter Queue (DLQ)", f"{kpis['total_dlq']}", delta=f"{kpis['failure_rate']}% fail rate", delta_color="inverse")

st.markdown("---")

# Section 1 & 2: Velocity / Spike Detection & Sentiment Distribution
left_col, right_col = st.columns([3, 2])

with left_col:
    st.subheader("📈 Real-Time Velocity & Anomaly Spike Detection")
    st.caption("Tumbling Window: 1-minute aggregation with dynamic anomaly threshold")
    
    velocity_history = db.get_velocity_history(limit=50)
    if velocity_history:
        df_vel = pd.DataFrame(velocity_history)
        
        # Check if the latest point is an anomaly
        is_latest_spike = df_vel.iloc[-1].get("is_spike", 0) == 1
        if is_latest_spike:
            st.error("🚨 **TRAFFIC ANOMALY DETECTED!** Incoming comment velocity exceeds 2x historical moving average.")
        else:
            st.success("🟢 **Traffic Flow Normal.** Consumer lag within healthy threshold.")

        chart = alt.Chart(df_vel).mark_line(point=True, color="#1f77b4").encode(
            x=alt.X("iso_time:N", title="Time Window (UTC)"),
            y=alt.Y("velocity_per_min:Q", title="Comments / Minute"),
            tooltip=["iso_time", "velocity_per_min", "event_count", "is_spike"]
        ).properties(height=260)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Waiting for incoming stream traffic... Send sample events to populate.")

with right_col:
    st.subheader("🎭 5-Min Sentiment Breakdown")
    st.caption("Sliding Window: Intent & Sentiment Classification Distribution")
    
    sentiment_data = pd.DataFrame({
        "Sentiment": ["Positive", "Negative", "Neutral", "Spam / Filtered"],
        "Count": [kpis["positive_count"], kpis["negative_count"], kpis["neutral_count"], kpis["spam_count"]],
        "Color": ["#2ca02c", "#d62728", "#7f7f7f", "#ff7f0e"]
    })

    if sentiment_data["Count"].sum() > 0:
        donut = alt.Chart(sentiment_data).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field="Count", type="quantitative"),
            color=alt.Color(field="Sentiment", type="nominal", scale=alt.Scale(
                domain=["Positive", "Negative", "Neutral", "Spam / Filtered"],
                range=["#28a745", "#dc3545", "#6c757d", "#fd7e14"]
            )),
            tooltip=["Sentiment", "Count"]
        ).properties(height=260)
        st.altair_chart(donut, use_container_width=True)
    else:
        st.info("No moderation actions recorded yet.")

# Section 3: Live Incident & Dead Letter Queue Monitor
st.markdown("---")
st.subheader("🛡️ Resilient Egress & Dead Letter Queue (DLQ) Incidents")
dlq_records = db.get_recent_dlq(limit=6)

if dlq_records:
    df_dlq = pd.DataFrame(dlq_records)
    st.warning(f"⚠️ Displaying latest {len(df_dlq)} poison-pill / failed messages isolated in topic 'dead_letter':")
    st.dataframe(df_dlq, use_container_width=True, hide_index=True)
else:
    st.success("✅ Zero messages in Dead Letter Queue. Downstream Facebook API connection is healthy.")

# Section 4: Live Moderation Stream Log
st.markdown("---")
st.subheader("📋 Recent Moderation Stream Activity")
recent_logs = db.get_recent_moderation_logs(limit=10)
if recent_logs:
    st.dataframe(pd.DataFrame(recent_logs), use_container_width=True, hide_index=True)
else:
    st.caption("No processed reply commands logged yet.")

# Auto-refresh loop
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()

