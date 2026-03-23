import os
import threading

# ── Server socket ────────────────────────────────────────────
# Render injects PORT automatically; default to 10000 (Render's default)
port  = os.environ.get("PORT", "10000")
bind  = f"0.0.0.0:{port}"

# ── Worker config ────────────────────────────────────────────
# MUST stay at 1 worker — oi_data / oi_history live in memory.
# Multiple workers = each has its own copy = data is split.
workers = 1
threads = 4
timeout = 120

# Log to stdout so Render's log panel shows everything
accesslog = "-"
errorlog  = "-"
loglevel  = "info"


# ── Thread startup hook ──────────────────────────────────────
# post_fork() is called inside the WORKER process after it is
# forked from the master.  This is the only safe place to start
# threads under gunicorn — threads started before the fork die
# silently and are never restarted.
def post_fork(server, worker):
    server.log.info(f"[post_fork pid={os.getpid()}] Starting OI + LTP threads …")
    from oi_dashboard import start_background_threads
    start_background_threads()
    server.log.info(f"[post_fork pid={os.getpid()}] Threads started.")
