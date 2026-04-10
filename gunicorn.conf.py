"""Gunicorn configuration for clausius.

Single worker with many threads: the app relies on shared in-memory state
(SSH pool, job cache, partition cache, progress cache) that cannot be split
across processes.  One process with 32 threads handles I/O-bound SSH work
well while keeping all caches consistent.
"""

import threading

from server.config import APP_PORT

bind = f"0.0.0.0:{APP_PORT}"
workers = 1
worker_class = "gthread"
threads = 32
timeout = 45
graceful_timeout = 10
keepalive = 5
max_requests = 0

accesslog = None
errorlog = "-"
loglevel = "info"


def post_fork(server, worker):
    from app import _run_init
    _run_init()
