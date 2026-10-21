"""The API's only way to data: analytics/metrics.py (small cell suppression) on the serving snapshot.

The serving snapshot (analytics/serving.py, `build.py snapshot`) is a slim copy of the warehouse with only the tables
the semantic layer reads. This engine reads data/analytics/serving/current.json to find it, so the nightly build and
the API never open the same file. A new snapshot is picked up on the next request.

mf runs one query at a time (a lock: two mf processes on one DuckDB file would fight over its lock), and answers are
cached per snapshot version and query, so a repeated query, including the saved queries warmed at startup, is
answered from memory.
"""
import sys
import threading
from collections import OrderedDict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "analytics"))
import metrics  # noqa: E402
import serving  # noqa: E402


class NoSnapshot(RuntimeError):
    pass


class Engine:
    def __init__(self, serving_dir, runner=None, cat=None, saved=None, cache_size=256, warm_in_background=True):
        self.serving_dir = Path(serving_dir)
        self.runner = runner or metrics.query
        self.cat = cat if cat is not None else metrics.catalog()
        self.saved = saved if saved is not None else metrics.saved_queries()
        self.cache, self.cache_size = OrderedDict(), cache_size
        self.lock = threading.Lock()
        self.warm_in_background = warm_in_background
        self._snap, self._mtime = None, None

    def snapshot(self):
        """The current snapshot pointer, re-read when current.json changes. Raises NoSnapshot."""
        pointer = self.serving_dir / serving.POINTER
        try:
            mtime = pointer.stat().st_mtime_ns
        except OSError:
            raise NoSnapshot("no serving snapshot yet: python3 analytics/build.py snapshot") from None
        if mtime != self._mtime:
            snap = serving.read_pointer(self.serving_dir)
            if not snap:
                raise NoSnapshot("the serving pointer is unreadable")
            changed = not self._snap or snap["version"] != self._snap["version"]
            self._snap, self._mtime = snap, mtime
            if changed:
                self.warm(background=self.warm_in_background)
        return self._snap

    def run(self, metric_names, group_by, where):
        """Suppressed rows for one query, and the snapshot they come from."""
        snap = self.snapshot()
        key = (snap["version"], tuple(metric_names), tuple(group_by), tuple(where))
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key], snap
            rows = self.runner(list(metric_names), list(group_by), list(where), list(group_by), db=snap["db"],
                               target=snap["target"], parse=False, cat=self.cat, complementary=True)
            self.cache[key] = rows
            while len(self.cache) > self.cache_size:
                self.cache.popitem(last=False)
        return rows, snap

    def run_saved(self, name):
        m, g, w = metrics.resolve_saved_query(name, self.saved)
        return (m, g) + self.run(m, g, w)

    def warm(self, background=False):
        """Answer every saved query once, so the first caller gets it from the cache."""
        def go():
            for name in self.saved:
                try:
                    self.run_saved(name)
                except Exception:   # a failing saved query fails again, visibly, when someone asks for it
                    pass
        if background:
            threading.Thread(target=go, daemon=True).start()
        else:
            go()
