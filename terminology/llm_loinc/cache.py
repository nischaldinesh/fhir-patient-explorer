"""A disk cache of model calls and a hard cap on calls per run.

One JSON file per call in data/llm_cache/<model>/<key[:2]>/<key>.json, where key is the sha256 of everything that
goes into the request (model, prompt version, system prompt, message, output schema, request settings). It stores
the model's text, the token usage the API reported and the measured latency, so a rerun answers from disk with the
original numbers and costs nothing.
"""
import hashlib
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path


def key_of(request):
    return hashlib.sha256(json.dumps(request, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class Cache:
    def __init__(self, root):
        self.root = Path(root)
        self.hits = self.misses = 0

    def path(self, model, key):
        return self.root / model / key[:2] / f"{key}.json"

    def get(self, model, key):
        p = self.path(model, key)
        if p.exists():
            self.hits += 1
            return json.loads(p.read_text(encoding="utf-8"))
        self.misses += 1
        return None

    def has(self, model, key):
        return self.path(model, key).exists()

    def put(self, model, key, entry):
        p = self.path(model, key)
        p.parent.mkdir(parents=True, exist_ok=True)
        entry = dict(entry, cached_at=datetime.now(timezone.utc).isoformat(timespec="seconds"))
        tmp = p.with_suffix(f".{os.getpid()}.part")
        tmp.write_text(json.dumps(entry, indent=1, sort_keys=True), encoding="utf-8")
        tmp.replace(p)


class Ledger:
    """Append only record of every API call actually sent (data/llm_cache/_ledger.jsonl): the source of truth for spend.

    The cache holds one answer per request; the ledger also counts repeats (the stability check) and anything else
    that reached the API.
    """

    def __init__(self, path):
        self.path = Path(path)
        self._lock = threading.Lock()

    def add(self, **entry):
        line = json.dumps(dict(entry, at=datetime.now(timezone.utc).isoformat(timespec="seconds")), sort_keys=True)
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def entries(self):
        if not self.path.exists():
            return []
        return [json.loads(x) for x in self.path.read_text(encoding="utf-8").splitlines() if x.strip()]


class CallCapReached(RuntimeError):
    pass


class Budget:
    """At most max_calls calls to the API in one run; the call that would pass it is never sent."""

    def __init__(self, max_calls):
        self.max_calls, self.calls = max_calls, 0
        self._lock = threading.Lock()

    def take(self):
        with self._lock:
            if self.calls >= self.max_calls:
                raise CallCapReached(f"call cap of {self.max_calls} reached; rerun to continue from the cache, "
                                     "or raise --max-calls")
            self.calls += 1
