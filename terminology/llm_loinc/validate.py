"""Check a ranker's answer against the candidate list and the LOINC table.

A code the model returns is kept only when it is one of the candidates it was shown and exists in Loinc.csv, so an
invented code can never reach the review queue or the eval. A deprecated code is replaced by its MapTo.csv target.
Every rejected code is recorded as a violation, so the eval can report how often the guard had to step in.
"""
from dataclasses import dataclass, field

NONE_ANSWERS = {"", "NONE", "NULL", "N/A"}


@dataclass
class Decision:
    choice: str | None
    top3: list = field(default_factory=list)
    confidence: float | None = None
    reason: str = ""
    violations: list = field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: float | None = None
    cached: bool = False
    error: str | None = None
    dry_run: bool = False


def _clean(code):
    c = (code or "").strip()
    return None if c.upper() in NONE_ANSWERS else c


def check_code(code, allowed, index, violations):
    """The code to keep (possibly mapped through MapTo.csv), or None. Appends to violations when it rejects."""
    c = _clean(code)
    if c is None:
        return None
    if c not in allowed:
        violations.append(f"{c}: not in the candidate list")
        return None
    status = index.status(c)
    if status is None:
        violations.append(f"{c}: not in Loinc.csv")
        return None
    if status == "DEPRECATED":
        targets = [t for t in index.mapto.get(c, ()) if index.status(t)]
        if not targets:
            violations.append(f"{c}: deprecated with no MapTo target")
            return None
        return targets[0]
    return c


def validate(raw, candidates, index):
    """raw: parsed model answer {choice, top3, confidence, reason}; candidates: the list the model was shown."""
    allowed = {c["code"] for c in candidates}
    violations = []
    choice = check_code(raw.get("choice"), allowed, index, violations)
    top3 = []
    for code in raw.get("top3") or []:
        kept = check_code(code, allowed, index, violations)
        if kept and kept not in top3:
            top3.append(kept)
    if choice and choice not in top3:
        top3.insert(0, choice)
    conf = raw.get("confidence")
    conf = None if conf is None else max(0.0, min(1.0, float(conf)))
    return Decision(choice=choice, top3=top3[:3], confidence=conf, reason=(raw.get("reason") or "").strip(),
                    violations=violations)
