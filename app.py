"""clausius — entry point.

Run standalone:  python app.py
Run production:  gunicorn -c gunicorn.conf.py app:app
"""

import logging
import logging.config
import os
import threading
import time

from flask import Flask

from server.config import APP_PORT, PROJECT_ROOT
from server.routes import api


def _configure_logging():
    log_dir = os.path.join(PROJECT_ROOT, "data")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "clausius.log")
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": log_path,
                "maxBytes": 5 * 1024 * 1024,
                "backupCount": 3,
                "formatter": "standard",
                "encoding": "utf-8",
            },
            "stderr": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
            },
        },
        "loggers": {
            "server": {
                "level": "INFO",
                "handlers": ["file", "stderr"],
                "propagate": False,
            },
        },
        "root": {
            "level": "WARNING",
            "handlers": ["file", "stderr"],
        },
    })


_configure_logging()

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024
app.register_blueprint(api)

_BOOT_TS = str(int(time.time()))


@app.context_processor
def _inject_static_version():
    return {"v": _BOOT_TS}


def _run_init():
    from server.db import init_db, cleanup_local_on_startup
    from server.logbooks import migrate_legacy_files
    from server.ssh import ssh_pool_gc_loop
    from server.backup import backup_loop
    from server.mounts import mount_health_loop
    from server.wds import wds_snapshot_loop
    from server.config import cache_gc_loop

    init_db()
    migrate_legacy_files()
    cleanup_local_on_startup()
    threading.Thread(target=ssh_pool_gc_loop, daemon=True).start()
    threading.Thread(target=backup_loop, daemon=True).start()
    threading.Thread(target=mount_health_loop, daemon=True).start()
    threading.Thread(target=wds_snapshot_loop, daemon=True).start()
    threading.Thread(target=cache_gc_loop, daemon=True).start()


if __name__ == "__main__":
    _run_init()
    app.run(host="0.0.0.0", port=APP_PORT, debug=False, threaded=True)
