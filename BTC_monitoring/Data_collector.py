"""
tx_collector.py — Real-time Bitcoin on-chain transaction monitor.

Streams unconfirmed transactions from blockchain.info's public WebSocket
(no API key, no polling — the server pushes each tx as it is broadcast),
flags unusually large transactions, and writes to SQLite.

Usage:
    python tx_collector.py                # CLI streamer
    # or embedded (from app_tx.py): run in a thread via tx_stream_loop()

Note on detection: BTC amounts are heavy-tailed, so the z-score is computed
on log10(amount) — a few-thousand-BTC tx is anomalous, a 0.001 vs 0.002 BTC
tx is not.
"""

import argparse
import asyncio
import json
import logging
import math
import sqlite3
import sys
import threading
from datetime import datetime
from pathlib import Path

import websockets

DB_PATH = Path(__file__).parent / "crypto.db"
WS_URL = "wss://ws.blockchain.info/inv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("tx_collector")

SCHEMA = """
    CREATE TABLE IF NOT EXISTS transactions (
        ts TEXT,
        hash TEXT PRIMARY KEY,
        amount_btc REAL,
        n_inputs INTEGER,
        n_outputs INTEGER,
        size INTEGER,
        z REAL,
        anomaly INTEGER
    )
"""


class LogAmountDetector:
    """Streaming z-score detector on log10(amount) — robust for heavy-tailed data."""

    def __init__(self, window_size: int = 500, z_thresh: float = 4.0, warmup: int = 50):
        self.window = []          # recent log10 amounts (bounded below)
        self.window_size = window_size
        self.z_thresh = z_thresh
        self.warmup = warmup

    def update(self, amount_btc: float) -> dict:
        la = math.log10(max(amount_btc, 1e-12))
        n = len(self.window)
        z = 0.0
        anomaly = False
        if n >= self.warmup:
            mean = sum(self.window) / n
            var = sum((x - mean) ** 2 for x in self.window) / n
            std = math.sqrt(var)
            if std > 0:
                z = (la - mean) / std
                anomaly = abs(z) > self.z_thresh
        self.window.append(la)
        if len(self.window) > self.window_size:
            self.window.pop(0)
        return {"z": round(z, 2), "anomaly": anomaly}


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(SCHEMA)
    conn.commit()


def parse_tx(data: dict) -> dict | None:
    """Extract the fields we care about from a blockchain.info 'utx' message."""
    tx = data.get("x", {})
    outs = tx.get("out") or []
    total_sat = sum(o.get("value", 0) or 0 for o in outs)
    if total_sat <= 0:
        return None  # e.g. OP_RETURN-only outputs
    return {
        "ts": datetime.fromtimestamp(tx["time"]).isoformat(timespec="seconds"),
        "hash": tx["hash"],
        "amount_btc": round(total_sat / 1e8, 8),
        "n_inputs": len(tx.get("inputs") or []),
        "n_outputs": len(outs),
        "size": tx.get("size", 0),
    }


async def tx_stream_loop(stop_event: threading.Event, db_path: Path = DB_PATH,
                         z_thresh: float = 4.0) -> None:
    """Connect to the WebSocket and stream transactions until stop_event is set.
    Reconnects automatically with backoff if the connection drops."""
    conn = sqlite3.connect(db_path)
    init_db(conn)
    detector = LogAmountDetector(z_thresh=z_thresh)
    backoff = 1
    total = 0

    while not stop_event.is_set():
        try:
            log.info("Connecting to %s ...", WS_URL)
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"op": "unconfirmed_sub"}))
                log.info("Subscribed — streaming unconfirmed transactions.")
                backoff = 1
                async for raw in ws:
                    if stop_event.is_set():
                        break
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if data.get("op") != "utx":
                        continue
                    tx = parse_tx(data)
                    if tx is None:
                        continue
                    stats = detector.update(tx["amount_btc"])
                    conn.execute(
                        "INSERT OR IGNORE INTO transactions VALUES (?,?,?,?,?,?,?,?)",
                        (tx["ts"], tx["hash"], tx["amount_btc"], tx["n_inputs"],
                         tx["n_outputs"], tx["size"], stats["z"],
                         int(stats["anomaly"])),
                    )
                    conn.commit()
                    total += 1
                    if stats["anomaly"]:
                        log.warning("ANOMALY: %.4f BTC in tx %s... (z=%.2f)",
                                    tx["amount_btc"], tx["hash"][:10], stats["z"])
                    elif total % 25 == 0:
                        log.info("%d txs seen | last: %.4f BTC (%d in / %d out)",
                                 total, tx["amount_btc"], tx["n_inputs"], tx["n_outputs"])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if stop_event.is_set():
                break
            log.error("WebSocket error (%s). Reconnecting in %ds...", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)

    log.info("TX collector stopped (%d transactions seen).", total)


def _run(stop_event: threading.Event, db_path: Path, z_thresh: float) -> None:
    asyncio.run(tx_stream_loop(stop_event, db_path, z_thresh))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--z", type=float, default=4.0)
    args = p.parse_args()
    try:
        _run(threading.Event(), DB_PATH, args.z)
    except KeyboardInterrupt:
        log.info("Stopped by user.")


if __name__ == "__main__":
    main()