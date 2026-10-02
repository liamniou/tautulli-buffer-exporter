#!/usr/bin/env python3
"""
Prometheus exporter for per-user Plex buffering events.

Tautulli tracks a `buffer_count` per active stream in its `sessions` table
(the count of times Plex reported the stream buffering). That value is only
kept while the session is active and is never persisted to history, so this
exporter polls the table and turns the per-session counts into a monotonic
per-user counter that Prometheus can alert on.

Metrics:
  plex_user_buffer_events_total{user="..."}  - cumulative buffering events
  plex_user_active_sessions{user="..."}       - current active streams
  tautulli_buffer_exporter_up                  - 1 if the last scrape succeeded
"""

import os
import sqlite3
import time
import logging

from prometheus_client import start_http_server, Counter, Gauge

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("TAUTULLI_DB", "/tautulli/tautulli.db")
SERVE_PORT = int(os.environ.get("SERVE_PORT", "9489"))
SCRAPE_INTERVAL = int(os.environ.get("SCRAPE_INTERVAL", "30"))

buffer_events = Counter(
    "plex_user_buffer_events",
    "Cumulative Plex buffering events per user (derived from Tautulli sessions)",
    ["user"],
)
active_sessions = Gauge(
    "plex_user_active_sessions",
    "Current number of active Plex streams per user",
    ["user"],
)
exporter_up = Gauge(
    "tautulli_buffer_exporter_up",
    "1 if the last scrape of the Tautulli database succeeded, else 0",
)

# session_key -> last observed buffer_count, so we only count new buffering events.
_last_buffer = {}


def read_sessions():
    """Return list of (session_key, user, buffer_count) for active streams."""
    # Open read-only in intent (query_only) but with a writable handle so SQLite
    # can take the shared-memory read locks required by WAL mode. We never write
    # to Tautulli's data.
    conn = sqlite3.connect(DB_PATH, timeout=5)
    try:
        conn.execute("PRAGMA query_only = ON")
        rows = conn.execute(
            "SELECT session_key, user, COALESCE(buffer_count, 0) FROM sessions"
        ).fetchall()
    finally:
        conn.close()
    return rows


def update_metrics():
    rows = read_sessions()

    current_keys = set()
    per_user_sessions = {}

    for session_key, user, buffer_count in rows:
        user = user or "unknown"
        current_keys.add(session_key)
        per_user_sessions[user] = per_user_sessions.get(user, 0) + 1

        previous = _last_buffer.get(session_key)
        if previous is None:
            # First time we see this session: adopt its current count as the
            # baseline so pre-existing buffers (e.g. exporter just started) are
            # not retroactively counted.
            delta = 0
        elif buffer_count >= previous:
            delta = buffer_count - previous
        else:
            # Counter went down -> the session row was reused for a new stream.
            delta = buffer_count

        if delta > 0:
            buffer_events.labels(user=user).inc(delta)

        _last_buffer[session_key] = buffer_count

    # Forget sessions that are no longer active.
    for stale_key in set(_last_buffer) - current_keys:
        del _last_buffer[stale_key]

    # Reset active-session gauges; only export users that currently stream.
    active_sessions.clear()
    for user, count in per_user_sessions.items():
        active_sessions.labels(user=user).set(count)


def main():
    logger.info("Starting Tautulli buffer exporter on port %s (db=%s)", SERVE_PORT, DB_PATH)
    start_http_server(SERVE_PORT)

    while True:
        try:
            update_metrics()
            exporter_up.set(1)
        except Exception as exc:  # noqa: BLE001 - report any scrape failure as down
            exporter_up.set(0)
            logger.error("Error updating metrics: %s", exc)
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    main()
