"""
pymongo is a runtime dependency of pipeline/intent_events.py (imported
transitively via pipeline.rollups -> intent_engine.url_normalizer, and
directly by intent_engine.repository for _ensure_column_exists reuse) but
isn't needed by any pure-logic test in this directory. Stub it so these
tests can run in environments without pymongo installed (it's always
present in the actual Docker image via requirements.txt).
"""

import sys
import types

if "pymongo" not in sys.modules:
    stub = types.ModuleType("pymongo")
    stub.MongoClient = object
    sys.modules["pymongo"] = stub
