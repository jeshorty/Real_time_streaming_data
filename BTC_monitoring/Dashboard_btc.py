"""
app_tx.py — All-in-one: real-time Bitcoin transaction stream + dashboard.

    streamlit run app_tx.py

Streams unconfirmed on-chain transactions (blockchain.info WebSocket) in a
background thread and shows a live dashboard. Ctrl+C stops everything.
"""

import sqlite3
import threading
import time
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import Data_collector as dcol

DB_PATH = Path(__file__).parent / "crypto.db"
REFRESH_SECONDS = 3

st.set_page_config(page_title="BTC Transaction Stream", layout="wide")
st.title("Real-Time Bitcoin Transaction Stream")
st.caption("Unconfirmed on-chain transactions via blockchain.info WebSocket")

# ---- Start the TX stream thread exactly once per session ----
if "tx_started" not in st.session_state:
    stop_event = threading.Event()
    t = threading.Thread(
        target=dcol._run,
        args=(stop_event, DB_PATH, 4.0),
        daemon=True,
        name="tx-collector-thread",
    )
    t.start()
    st.session_state["tx_started"] = True
    st.toast("Transaction stream started", icon="")

# ---- Read data (wait for first transactions) ----
conn = sqlite3.connect(DB_PATH)
conn.execute(dcol.SCHEMA)  # ensure table exists so first read never crashes
df = pd.read_sql("SELECT * FROM transactions ORDER BY ts", conn, parse_dates=["ts"])
conn.close()

if df.empty:
    st.info("Waiting for the first transactions — usually within seconds...")
    time.sleep(3)
    st.rerun()

total = len(df)
recent = df.tail(1000)  # recent window for charts/stats
last = df.iloc[-1]
tx_last_60s = int((recent["ts"] > pd.Timestamp.now() - pd.Timedelta(seconds=60)).sum())
silent_s = (pd.Timestamp.now() - df["ts"].iloc[-1]).total_seconds()
anoms = df[df["anomaly"].astype(bool)]

# ---- KPI row ----
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Transactions seen", f"{total:,}")
c2.metric("Tx in last 60s", tx_last_60s)
c3.metric("Last tx amount", f"{last['amount_btc']:.4f} BTC")
c4.metric("Anomalies", f"{len(anoms):,}")
c5.metric("Stream lag", f"{silent_s:.0f}s ago",
          delta=None if silent_s < 30 else "STALE", delta_color="inverse")

if silent_s > 60:
    st.error(f"No transactions for {silent_s:.0f}s — the stream may be down "
             "(check the terminal for reconnection logs).")

# ---- Tx rate chart (10-second buckets) ----
st.subheader("Transaction rate")
bucket = recent.set_index("ts").resample("10s").size().reset_index(name="txs")
rate_fig = go.Figure()
rate_fig.add_bar(x=bucket["ts"], y=bucket["txs"], name="txs / 10s")
rate_fig.update_layout(height=250, xaxis_title="Time", yaxis_title="Transactions")
st.plotly_chart(rate_fig, use_container_width=True)

# ---- Recent amounts with anomalies highlighted ----
st.subheader("Recent transaction amounts (BTC)")
show = df.tail(40)
amt_fig = go.Figure()
colors = ["red" if a else "#1f77b4" for a in show["anomaly"].astype(bool)]
amt_fig.add_bar(x=show["ts"], y=show["amount_btc"],
                marker_color=colors, name="amount")
amt_fig.update_layout(height=280, xaxis_title="Time", yaxis_title="BTC")
st.plotly_chart(amt_fig, use_container_width=True)

# ---- Recent transactions table ----
st.subheader("Latest transactions")
table = df.tail(25).iloc[::-1].copy()
table["hash"] = table["hash"].str[:14] + "..."
st.dataframe(
    table[["ts", "hash", "amount_btc", "n_inputs", "n_outputs", "z", "anomaly"]],
    use_container_width=True,
    hide_index=True,
)

if not anoms.empty:
    st.subheader("Anomaly log (unusually large transactions)")
    at = anoms.tail(20).iloc[::-1].copy()
    at["hash"] = at["hash"].str[:14] + "..."
    st.dataframe(at[["ts", "hash", "amount_btc", "z"]],
                 use_container_width=True, hide_index=True)

st.caption(f"Auto-refresh every {REFRESH_SECONDS}s — stream logs in this terminal.")

time.sleep(REFRESH_SECONDS)
st.rerun()