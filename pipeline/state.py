"""
Shared mutable state and infra singletons for the Intent Data Aggregation
pipeline.

This is the single source of truth for brand-state dicts (populated by
initialize_brand_configs, which stays in aws_background.py) and
process-wide singletons (logger, Flask app, APScheduler scheduler, shared
HTTP session, etc). Every other pipeline module imports what it needs from
here rather than owning its own copy, so mutations made by one module
(e.g. initialize_brand_configs populating brand_db_configs) are visible to
every other module.

Only names that are mutated IN PLACE (dict/list writes, Lock/Queue/
threading.local usage) live here. Module-level scalars that get
REASSIGNED via a `global` statement must stay co-located with the function
that reassigns them, since reassigning a name imported via
`from pipeline.state import x` only rebinds the importing module's own
copy, not the shared one.
"""

import logging
import queue
import threading
from collections import deque
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

# ---- Timezone ----
IST = ZoneInfo("Asia/Kolkata")

# ---- Logging ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("intent_pipeline")


class _LastLogsHandler(logging.Handler):
    def __init__(self, maxlen: int = 2) -> None:
        super().__init__()
        self._buffer = deque(maxlen=maxlen)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self._buffer.append(msg)
        except Exception:
            # Avoid breaking logging on handler errors.
            pass

    def get_recent(self) -> List[str]:
        return list(self._buffer)


_last_logs_handler = _LastLogsHandler(maxlen=2)
_last_logs_handler.setLevel(logging.INFO)
_last_logs_handler.setFormatter(
    logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
logging.getLogger().addHandler(_last_logs_handler)

# ---- Brand-state (populated in place by initialize_brand_configs) ----
brand_timezones: Dict[int, ZoneInfo] = {}
brand_tag_to_index_map: Dict[str, int] = {}
brand_id_from_config: Dict[int, int] = {}  # brand_index -> brand_id from brand-config source
brand_db_configs: Dict[int, Dict[str, Any]] = {}
sqlalchemy_engines: Dict[int, Any] = {}
active_brand_indices: List[int] = []  # only brands with valid configs/engines

# ---- App/scheduler singletons ----
scheduler = BackgroundScheduler(timezone=IST)
app = Flask("intent_pipeline")

# ---- Manual-trigger plumbing ----
MANUAL_TRIGGER_QUEUE = queue.Queue()

# ---- DB connection metrics/shutdown infra ----
_db_metrics_lock = threading.Lock()
_db_shutdown_lock = threading.Lock()
_db_context = threading.local()
_db_connection_metrics: Dict[str, int] = {
    "active_connector_connections": 0,
    "peak_connector_connections": 0,
    "connections_opened_total": 0,
    "connection_failures_total": 0,
    "active_sqlalchemy_connections": 0,
    "peak_sqlalchemy_connections": 0,
}

# ---- Shared HTTP session (connection pooling for outbound requests) ----
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

http_session = requests.Session()
retry_strategy = Retry(
    total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(
    pool_connections=20, pool_maxsize=50, max_retries=retry_strategy, pool_block=False
)
http_session.mount("https://", adapter)
http_session.mount("http://", adapter)
