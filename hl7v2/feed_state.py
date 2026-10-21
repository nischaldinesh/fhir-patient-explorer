"""Persistent state of the live Riverside General feed, kept between daily runs as one JSON file.

The daily generator reads it to know who is in hospital and who has been here before, and the router
reads the recent control IDs so a resend is still caught as a duplicate on the next day. The pipeline
saves it only after a successful load, so a failed day can be rerun and produces the same messages.
"""
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from pathlib import Path

DEFAULT_PATH = Path("data/hl7v2/feed_state.json")
FIRST_MRN = 200000  # RV100200 to RV100368 belong to the original 25 patients; the live feed starts here
SEEN_DAYS = 30      # how long control IDs are remembered for duplicate detection
DISCHARGE_DAYS = 90  # how long past discharges are kept to choose returning patients


@dataclass
class FeedState:
    last_day: str = ""                                  # ISO date of the last generated day, "" before the first
    next_mrn: int = FIRST_MRN
    roster: list = field(default_factory=list)          # every live feed patient: mrn, demographics, comorbid
    open_admissions: list = field(default_factory=list)  # visit, mrn, admit, unit, diagnoses, planned discharge
    discharges: list = field(default_factory=list)      # visit, mrn, discharged
    seen_control_ids: dict = field(default_factory=dict)  # control ID -> ISO day it was received
    carryover: list = field(default_factory=list)       # resends that arrive just after midnight, next day

    def remember(self, control_ids, day: date):
        """Add the day's control IDs and forget those older than SEEN_DAYS."""
        for cid in sorted(control_ids):
            self.seen_control_ids.setdefault(cid, day.isoformat())
        cutoff = (day - timedelta(days=SEEN_DAYS)).isoformat()
        self.seen_control_ids = {c: d for c, d in self.seen_control_ids.items() if d > cutoff}

    def prune_discharges(self, day: date):
        cutoff = (day - timedelta(days=DISCHARGE_DAYS)).strftime("%Y%m%d")
        self.discharges = [d for d in self.discharges if d["discharged"][:8] > cutoff]

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=1, sort_keys=True)

    @classmethod
    def from_json(cls, text: str) -> "FeedState":
        return cls(**json.loads(text))


def load(path=DEFAULT_PATH) -> FeedState:
    path = Path(path)
    return FeedState.from_json(path.read_text(encoding="utf-8")) if path.exists() else FeedState()


def save(state: FeedState, path=DEFAULT_PATH):
    """Write to a temporary file first, so a crash never leaves half a state file behind."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(state.to_json(), encoding="utf-8")
    os.replace(tmp, path)
