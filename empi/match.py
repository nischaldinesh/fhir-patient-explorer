#!/usr/bin/env python3
"""Enterprise Master Patient Index (EMPI), demo: find the same person across sources and link the records.

A hospital receives the same person from several systems (here MIMIC, Synthea and the Riverside General HL7 v2
feed), each with its own id and its own spelling. The matcher compares every likely pair and decides:

  blocking   only pairs that share birth year plus the Soundex of the last name, or the exact birth date, are compared
  scoring    a weighted comparison of each field (Jaro Winkler for names, a nickname table for first names, birth
             date exact or with two digits swapped, sex, ZIP code, phone, street address) gives 0 to 100, with the
             reason for every field
  deciding   90 and above: linked automatically; 70 to 89: a person reviews the pair; under 70: not the same person
             (empi/config.json holds the weights and thresholds)
  linking    both Patient resources get Patient.link (type seealso) pointing at each other. Records are never merged
             or deleted, and a pair that is already linked is left alone, so a rerun changes nothing

MIMIC records are deidentified (no name, shifted dates), so they cannot be matched and are skipped, as they should be.

Commands:
  run      fetch every Patient, score the candidate pairs, link the automatic matches and the accepted pairs, and
           publish the review queue to Binary/empi-review (with the quality measured against Binary/empi-truth,
           the seeded test set from hl7v2/pipeline.py empi-seed). --dry-run writes data/empi/review.json only.
  decide   accept or reject one pair in review; an accepted pair is linked like an automatic match
  evaluate precision and recall against another ground truth, such as a held out set (pipeline.py empi-seed
           --seed 7 writes data/empi/truth-seed7.json). Scores the server's patients afresh and writes nothing.

Writes go to HAPI directly on localhost (http://localhost:8080/fhir), never through a public proxy.

Usage:
  python3 empi/match.py run [--base URL] [--dry-run]
  python3 empi/match.py decide <pair id> --accept|--reject [--base URL]
  python3 empi/match.py evaluate --truth data/empi/truth-seed7.json [--base URL]
"""
import argparse
import base64
import csv
import hashlib
import json
import os
import re
import sys
import unicodedata
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
from tools import DEFAULT_BASE, FRESH, http  # noqa: E402

CONFIG = HERE / "config.json"
NICKNAMES = HERE / "nicknames.csv"
OUT = Path("data/empi")
TRUTH_FILE = OUT / "truth.json"
REVIEW_ID = "empi-review"
TRUTH_ID = "empi-truth"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}

REPO = "https://github.com/nischaldinesh/fhir-patient-explorer"
HL7V2_TAG = (f"{REPO}/CodeSystem/source", "hl7v2")
TEST_SET_TAG = {"system": f"{REPO}/CodeSystem/empi", "code": "empi-test-set", "display": "EMPI test set (demo)"}

FIELDS = [("last_name", "Last name"), ("first_name", "First name"), ("middle_name", "Middle name"),
          ("birth_date", "Birth date"), ("sex", "Sex"), ("phone", "Phone"), ("postal_code", "ZIP code"),
          ("address", "Street address")]
DECIDED = ("accepted", "rejected")
STREET_WORDS = {"street": "st", "avenue": "ave", "av": "ave", "road": "rd", "lane": "ln", "drive": "dr",
                "court": "ct", "boulevard": "blvd", "place": "pl", "terrace": "ter", "apartment": "unit",
                "apt": "unit", "suite": "unit", "north": "n", "south": "s", "east": "e", "west": "w"}


def load_config(path=CONFIG):
    return json.loads(Path(path).read_text(encoding="utf-8"))


# ---------- comparing strings ----------

def normalize_name(text):
    """Lowercase letters only: accents removed (Jesús -> jesus), digits dropped (Synthea's Kuhn96 -> kuhn),
    and no spaces, apostrophes or hyphens (O'Kon -> okon)."""
    plain = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z]", "", plain.lower())


_SOUNDEX = {**dict.fromkeys("bfpv", "1"), **dict.fromkeys("cgjkqsxz", "2"), **dict.fromkeys("dt", "3"),
            "l": "4", **dict.fromkeys("mn", "5"), "r": "6"}


def soundex(name):
    """American Soundex: the first letter and three digits (Robert R163, Tymczak T522, Pfister P236). H and W do
    not separate letters with the same code; vowels do. "" for a name without letters."""
    s = normalize_name(name)
    if not s:
        return ""
    out, last = s[0].upper(), _SOUNDEX.get(s[0], "")
    for ch in s[1:]:
        code = _SOUNDEX.get(ch, "")
        if code and code != last:
            out += code
        if ch not in "hw":
            last = code
    return (out + "000")[:4]


def jaro(a, b):
    if a == b:
        return 1.0 if a else 0.0
    if not a or not b:
        return 0.0
    window = max(max(len(a), len(b)) // 2 - 1, 0)
    used_b = [False] * len(b)
    matched_a = []
    for i, ch in enumerate(a):
        for j in range(max(0, i - window), min(len(b), i + window + 1)):
            if not used_b[j] and b[j] == ch:
                used_b[j] = True
                matched_a.append(ch)
                break
    m = len(matched_a)
    if not m:
        return 0.0
    matched_b = [b[j] for j in range(len(b)) if used_b[j]]
    transpositions = sum(x != y for x, y in zip(matched_a, matched_b)) / 2
    return (m / len(a) + m / len(b) + (m - transpositions) / m) / 3


def jaro_winkler(a, b, p=0.1):
    """Jaro similarity with a bonus for a shared prefix of up to 4 letters (Winkler): 1.0 the same, 0.0 nothing alike."""
    j = jaro(a, b)
    prefix = 0
    for x, y in zip(a[:4], b[:4]):
        if x != y:
            break
        prefix += 1
    return j + prefix * p * (1 - j)


def load_nicknames(path=NICKNAMES):
    """{name: set of the names it can stand for}, both ways: pam -> {pamela}, pamela -> {pam}."""
    names = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            full = normalize_name(row["name"])
            for nick in filter(None, (normalize_name(n) for n in row["nicknames"].split(";"))):
                names.setdefault(full, set()).add(nick)
                names.setdefault(nick, set()).add(full)
    return names


def is_nickname(a, b, nicknames):
    """True when one first name is a nickname of the other (Pam and Pamela), or both are nicknames of the same name
    (Ted and Ned, both Edward)."""
    a, b = normalize_name(a), normalize_name(b)
    if not a or not b or a == b:
        return False
    sa, sb = nicknames.get(a, set()), nicknames.get(b, set())
    return b in sa or a in sb or bool(sa & sb)


# ---------- reading a Patient ----------

def source_of(p):
    """hl7v2, mimic, synthea or unknown, the same rules as silver_patient."""
    if any((t.get("system"), t.get("code")) == HL7V2_TAG for t in p.get("meta", {}).get("tag", [])):
        return "hl7v2"
    urls = " ".join([*p.get("meta", {}).get("profile", []), *(i.get("system", "") for i in p.get("identifier", []))]).lower()
    if "mimic" in urls:
        return "mimic"
    if "synthetichealth" in urls or "synthea" in urls:
        return "synthea"
    return "unknown"


def _clean(text):
    """A name as people write it: Synthea's digits removed (Pamela262 -> Pamela)."""
    return re.sub(r"\d+$", "", (text or "").strip())


def _digits(text):
    return re.sub(r"\D", "", text or "")


def record(p):
    """The demographics the matcher compares, from a FHIR Patient. Placeholder names (MIMIC's Patient_10014729) count
    as no name. comparable is False when the last name, first name or full birth date is missing."""
    names = p.get("name") or [{}]
    n = next((x for x in names if x.get("use") == "official"), names[0])
    family = _clean(n.get("family"))
    if re.fullmatch(r"(?i)patient[ _]?\d*", n.get("family") or ""):
        family = ""
    given = [_clean(g) for g in n.get("given", []) if _clean(g)]
    phone = next((t.get("value", "") for t in p.get("telecom", []) if t.get("system") == "phone"), "")
    addr = (p.get("address") or [{}])[0]
    zip_ = _digits(addr.get("postalCode"))[:5]
    birth = p.get("birthDate") or ""
    mrn = next((i.get("value") for i in p.get("identifier", [])
                if any(c.get("code") == "MR" for c in i.get("type", {}).get("coding", []))), None)
    r = {
        "id": p["id"],
        "source": source_of(p),
        "mrn": mrn or (p.get("identifier") or [{}])[0].get("value"),
        "first": given[0] if given else "",
        "middle": given[1] if len(given) > 1 else "",
        "last": family,
        "birth_date": birth if re.fullmatch(r"\d{4}-\d{2}-\d{2}", birth) else "",
        "sex": p.get("gender") if p.get("gender") in ("male", "female") else "",
        "deceased": bool(p.get("deceasedDateTime") or p.get("deceasedBoolean")),
        "phone": _digits(phone)[-10:] if len(_digits(phone)) >= 7 else "",
        "phone_display": phone,
        "address": " ".join(addr.get("line", [])),
        "city": addr.get("city", ""),
        "postal_code": "" if zip_ in ("", "00000") else zip_,
        "test_set": any(t.get("code") == TEST_SET_TAG["code"] for t in p.get("meta", {}).get("tag", [])),
        "links": sorted(l.get("other", {}).get("reference", "").replace("Patient/", "") for l in p.get("link", [])),
    }
    r["comparable"] = bool(normalize_name(r["last"]) and normalize_name(r["first"]) and r["birth_date"])
    return r


# ---------- blocking ----------

def blocking_keys(r):
    """Birth year plus Soundex of the last name, and the exact birth date."""
    if not r["comparable"]:
        return set()
    return {f"y|{r['birth_date'][:4]}|{soundex(r['last'])}", f"d|{r['birth_date']}"}


def candidate_pairs(records):
    """Every pair of comparable records that shares a blocking key, once, as (a, b) with a's id before b's."""
    blocks = {}
    for r in records:
        for key in blocking_keys(r):
            blocks.setdefault(key, []).append(r)
    seen, out = set(), []
    for members in blocks.values():
        members = sorted(members, key=lambda x: x["id"])
        for i, a in enumerate(members):
            for b in members[i + 1:]:
                if (a["id"], b["id"]) not in seen and a["id"] != b["id"]:
                    seen.add((a["id"], b["id"]))
                    out.append((a, b))
    return sorted(out, key=lambda ab: (ab[0]["id"], ab[1]["id"]))


# ---------- comparing fields ----------
# Each returns (fraction of the field's weight earned, result code, reason). None as the fraction means the field is
# missing on one side: it is left out of the score, neither for nor against.

def compare_name(a, b, cfg, nicknames=None):
    x, y = normalize_name(a), normalize_name(b)
    if not x or not y:
        return None, "missing", "Missing on one side"
    if x == y:
        return 1.0, "same", "Same"
    if nicknames is not None and is_nickname(a, b, nicknames):
        return cfg["partial"]["nickname"], "nickname", f"{_clean(a)} and {_clean(b)} are the same name (nickname)"
    jw = jaro_winkler(x, y)
    if jw >= cfg["jaro_winkler"]["close_spelling"]:
        return cfg["partial"]["close_spelling"], "close_spelling", f"Close spelling (Jaro Winkler {jw:.2f})"
    if jw >= cfg["jaro_winkler"]["sounds_alike"] and soundex(x) == soundex(y):
        return cfg["partial"]["sounds_alike"], "sounds_alike", f"Sounds alike (Soundex {soundex(x)}, Jaro Winkler {jw:.2f})"
    return 0.0, "different", f"Different (Jaro Winkler {jw:.2f})"


def compare_middle(a, b, cfg):
    x, y = normalize_name(a), normalize_name(b)
    if not x or not y:
        return None, "missing", "Missing on one side"
    if x == y:
        return 1.0, "same", "Same"
    if (len(x) == 1 or len(y) == 1) and x[0] == y[0]:
        return 1.0, "same_initial", "Same initial"
    return 0.0, "different", "Different"


def transposed(a, b):
    """Two dates (YYYY-MM-DD) that differ only by two neighbouring digits swapped (1986-05-24 and 1986-05-42 would be
    one, if it were a date), or by day and month swapped (1986-05-04 and 1986-04-05)."""
    if not a or not b or a == b:
        return False
    da, db = a.replace("-", ""), b.replace("-", "")
    diff = [i for i in range(len(da)) if da[i] != db[i]]
    if len(diff) == 2 and diff[1] == diff[0] + 1 and da[diff[0]] == db[diff[1]] and da[diff[1]] == db[diff[0]]:
        return True
    return da[:4] == db[:4] and da[4:6] == db[6:8] and da[6:8] == db[4:6]


def compare_birth_date(a, b, cfg):
    if not a or not b:
        return None, "missing", "Missing on one side"
    if a == b:
        return 1.0, "same", "Same"
    if transposed(a, b):
        if a[:4] == b[:4] and a[5:7] == b[8:10] and a[8:10] == b[5:7]:
            return cfg["partial"]["transposed_birth_date"], "transposed", "Day and month swapped"
        return cfg["partial"]["transposed_birth_date"], "transposed", "Two digits swapped"
    return 0.0, "different", "Different"


def compare_exact(a, b, same="Same", different="Different"):
    if not a or not b:
        return None, "missing", "Missing on one side"
    return (1.0, "same", same) if a == b else (0.0, "different", different)


def normalize_street(text):
    words = re.sub(r"[^a-z0-9 ]+", " ", (text or "").lower()).split()
    return " ".join(STREET_WORDS.get(w, w) for w in words)


def compare_address(a, b, cfg):
    x, y = normalize_street(a), normalize_street(b)
    if not x or not y:
        return None, "missing", "Missing on one side"
    if x == y:
        return 1.0, "same", "Same"
    num_x, num_y = re.match(r"\d*", x).group(), re.match(r"\d*", y).group()
    if num_x and num_x == num_y and jaro_winkler(x, y) >= cfg["jaro_winkler"]["same_street"]:
        return cfg["partial"]["address_written_differently"], "written_differently", "Same address, written differently"
    return 0.0, "different", "Different"


# ---------- scoring a pair ----------

def _display(r, field):
    if field == "last_name":
        return r["last"]
    if field == "first_name":
        return r["first"]
    if field == "middle_name":
        return r["middle"]
    if field == "phone":
        return r["phone_display"] if r["phone"] else ""
    if field == "address":
        return ", ".join(x for x in (r["address"], r["city"]) if x)
    return r[field]


def score_pair(a, b, cfg, nicknames):
    """{"score": 0 to 100, "fields": [per field: values, points, max, result, reason]}. The score is the share of the
    weights earned, over the fields both records have."""
    w = cfg["weights"]
    results = {
        "last_name": compare_name(a["last"], b["last"], cfg),
        "first_name": compare_name(a["first"], b["first"], cfg, nicknames),
        "middle_name": compare_middle(a["middle"], b["middle"], cfg),
        "birth_date": compare_birth_date(a["birth_date"], b["birth_date"], cfg),
        "sex": compare_exact(a["sex"], b["sex"]),
        "phone": compare_exact(a["phone"], b["phone"], "Same number", "Different number"),
        "postal_code": compare_exact(a["postal_code"], b["postal_code"]),
        "address": compare_address(a["address"], b["address"], cfg),
    }
    earned = possible = 0.0
    fields = []
    for key, label in FIELDS:
        fraction, result, reason = results[key]
        points = None if fraction is None else round(w[key] * fraction, 1)
        if fraction is not None:
            earned += w[key] * fraction
            possible += w[key]
        fields.append({"field": key, "label": label, "a": _display(a, key), "b": _display(b, key),
                       "points": points, "max": w[key], "result": result, "reason": reason})
    return {"score": round(100 * earned / possible, 1) if possible else 0.0, "fields": fields}


def classify(score, thresholds):
    """auto_link at or above the auto link threshold, review at or above the review threshold, else no_match."""
    if score >= thresholds["auto_link"]:
        return "auto_link"
    if score >= thresholds["review"]:
        return "review"
    return "no_match"


def pair_id(a_id, b_id):
    """Stable and short, the same whichever record comes first: pair-3f2a9c1b07."""
    x, y = sorted((a_id, b_id))
    return "pair-" + hashlib.sha1(f"{x}|{y}".encode()).hexdigest()[:10]


def summary(r):
    """What the review page shows about one record."""
    name = " ".join(x for x in (r["first"], r["middle"], r["last"]) if x)
    return {"id": r["id"], "source": r["source"], "name": name, "mrn": r["mrn"], "birth_date": r["birth_date"],
            "test_set": r["test_set"]}


def match_all(records, cfg, nicknames, previous=None):
    """Score every candidate pair. A decision a person made earlier (accepted or rejected, kept in the previous review
    queue) stands, whatever the new score. Returns every pair, no matches included, sorted by score."""
    decided = {p["pair_id"]: p for p in (previous or []) if p.get("status") in DECIDED}
    pairs = []
    for a, b in candidate_pairs(records):
        s = score_pair(a, b, cfg, nicknames)
        pid = pair_id(a["id"], b["id"])
        p = {"pair_id": pid, "a": summary(a), "b": summary(b), "score": s["score"],
             "status": classify(s["score"], cfg["thresholds"]), "fields": s["fields"]}
        if pid in decided:
            p["status"] = decided[pid]["status"]
            p["decided_at"] = decided[pid].get("decided_at")
        pairs.append(p)
    return sorted(pairs, key=lambda p: (-p["score"], p["pair_id"]))


def to_link(pairs):
    """The pairs that should carry Patient.link: automatic matches and pairs a person accepted."""
    return [p for p in pairs if p["status"] in ("auto_link", "accepted")]


def counts(pairs):
    out = {s: 0 for s in ("auto_link", "review", "accepted", "rejected", "no_match")}
    for p in pairs:
        out[p["status"]] += 1
    out["candidate_pairs"] = len(pairs)
    return out


def quality(pairs, truth):
    """Precision and recall of the automatic links on the seeded test set (truth: hl7v2/pipeline.py empi-seed). Only
    pairs with a test set record count. None when there is no truth yet."""
    if not truth:
        return None
    pair_of = lambda t: frozenset((t["seed_id"], t["existing_id"]))  # noqa: E731
    matches = {pair_of(t) for t in truth if t["kind"] == "match"}
    negatives = {pair_of(t) for t in truth if t["kind"] == "hard_negative"}
    test_ids = {t["seed_id"] for t in truth}
    scoped = [p for p in pairs if p["a"]["id"] in test_ids or p["b"]["id"] in test_ids]
    auto = {frozenset((p["a"]["id"], p["b"]["id"])) for p in scoped if p["status"] == "auto_link"}
    review = {frozenset((p["a"]["id"], p["b"]["id"])) for p in scoped if p["status"] == "review"}
    tp, fp = len(auto & matches), len(auto - matches)
    return {
        "true_match_pairs": len(matches),
        "hard_negative_pairs": len(negatives),
        "auto_linked": len(auto),
        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": len(matches - auto),
        "precision": round(tp / len(auto), 3) if auto else None,
        "recall": round(tp / len(matches), 3) if matches else None,
        "hard_negatives_auto_linked": len(auto & negatives),
        "pairs_in_review": len(review),
        "true_matches_in_review": len(review & matches),
        "hard_negatives_in_review": len(review & negatives),
    }


def with_link(patient, other_id):
    """The Patient with a seealso link to other_id added, ready to PUT, or None when the link is already there."""
    target = f"Patient/{other_id}"
    if any(l.get("other", {}).get("reference") == target for l in patient.get("link", [])):
        return None
    p = json.loads(json.dumps(patient))
    for k in ("versionId", "lastUpdated"):
        p.get("meta", {}).pop(k, None)
    p["link"] = [*p.get("link", []), {"other": {"reference": target}, "type": "seealso"}]
    return p


def review_document(pairs, cfg, links, truth, generated_at):
    """Binary/empi-review: the counts, the quality on the test set and every pair that is not a clear no match
    (names and birth dates: protected health information, for the data engineer's /empi page)."""
    return {
        "contains_phi": True,
        "generated_at": generated_at,
        "thresholds": cfg["thresholds"],
        "weights": cfg["weights"],
        "counts": counts(pairs),
        "links": links,
        "quality": quality(pairs, truth),
        "pairs": [p for p in pairs if p["status"] != "no_match"],
    }


# ---------- the FHIR server ----------

def local_hosts(env=None):
    """LOCAL_HOSTS plus the comma separated names in EMPI_EXTRA_LOCAL_HOSTS (empty outside Docker). Local Airflow sets it
    to host.docker.internal, the throwaway HAPI as a container sees it."""
    extra = (os.environ if env is None else env).get("EMPI_EXTRA_LOCAL_HOSTS", "")
    return LOCAL_HOSTS | {h.strip().lower() for h in extra.split(",") if h.strip()}


def require_local(base):
    """EMPI writes go to HAPI directly on localhost, never through a public proxy."""
    host = urllib.parse.urlparse(base).hostname
    if host not in local_hosts():
        raise SystemExit(f"Refusing to write to {base}: EMPI links are written to HAPI on localhost only "
                         "(for example --base http://localhost:8080/fhir)")


def json_binary(rid, value):
    data = base64.b64encode(json.dumps(value).encode("utf-8")).decode("ascii")
    return {"resourceType": "Binary", "id": rid, "contentType": "application/json", "data": data}


def read_binary(base, rid):
    """The JSON a Binary holds, None when the server has no such Binary. Any other error is raised."""
    try:
        b = http("GET", f"{base}/Binary/{rid}", retries=2)
    except RuntimeError as e:
        if "HTTP 404" in str(e) or "HTTP 410" in str(e):
            return None
        raise
    return json.loads(base64.b64decode(b.get("data", "")) or "null")


def fetch_patients(base, page_size=500):
    """Every Patient, following the server's paging. A fresh search, so records loaded a moment ago are included."""
    out, path = [], f"Patient?_count={page_size}"
    while path:
        bundle = http("GET", f"{base}{path}" if path.startswith("?") else f"{base}/{path}", headers=FRESH)
        out += [e["resource"] for e in bundle.get("entry", []) if e.get("resource", {}).get("resourceType") == "Patient"]
        nxt = next((l.get("url", "") for l in bundle.get("link", []) if l.get("relation") == "next"), "")
        path = nxt[nxt.find("?"):] if "?" in nxt else None
    return out


def link_pair(base, a_id, b_id):
    """Patient.link both ways. Returns how many Patients were updated: 0 when the pair was already linked."""
    changed = 0
    for x, y in ((a_id, b_id), (b_id, a_id)):
        patched = with_link(http("GET", f"{base}/Patient/{x}", retries=2), y)
        if patched:
            http("PUT", f"{base}/Patient/{x}", patched)
            changed += 1
    return changed


def read_truth(base):
    """The seeded test set: Binary/empi-truth on the server, else data/empi/truth.json, else []."""
    t = None
    try:
        t = read_binary(base, TRUTH_ID)
    except RuntimeError:
        pass
    if t is None and TRUTH_FILE.exists():
        t = json.loads(TRUTH_FILE.read_text(encoding="utf-8"))
    return (t or {}).get("pairs", [])


def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def print_summary(doc):
    c, q = doc["counts"], doc["quality"]
    print(f"Candidate pairs {c['candidate_pairs']}: auto linked {c['auto_link']}, review {c['review']}, "
          f"accepted {c['accepted']}, rejected {c['rejected']}, no match {c['no_match']}")
    print(f"Links: {doc['links']['pairs_linked']} pairs, {doc['links']['patients_updated']} Patients updated now")
    if q:
        print(f"Test set: precision {q['precision']}, recall {q['recall']} ({q['true_positives']} of "
              f"{q['true_match_pairs']} true matches auto linked, {q['false_positives']} false), "
              f"{q['hard_negatives_auto_linked']} of {q['hard_negative_pairs']} hard negatives auto linked, "
              f"{q['pairs_in_review']} in review")
    else:
        print("Test set: no truth yet (python3 hl7v2/pipeline.py empi-seed)")


def save_local(doc, out=OUT):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "review.json").write_text(json.dumps(doc, indent=1), encoding="utf-8")


def cmd_run(a):
    base = a.base.rstrip("/")
    if not a.dry_run:
        require_local(base)
    cfg, nicknames = load_config(a.config), load_nicknames()
    patients = fetch_patients(base)
    records = [record(p) for p in patients]
    previous = (read_binary(base, REVIEW_ID) or {}).get("pairs", [])
    pairs = match_all(records, cfg, nicknames, previous)
    skipped = sum(1 for r in records if not r["comparable"])
    print(f"{len(records)} patients, {skipped} without a name and full birth date skipped (MIMIC is deidentified)")
    links = {"pairs_linked": len(to_link(pairs)), "patients_updated": 0}
    if not a.dry_run:
        for p in to_link(pairs):
            links["patients_updated"] += link_pair(base, p["a"]["id"], p["b"]["id"])
    doc = review_document(pairs, cfg, links, read_truth(base), now_utc())
    save_local(doc)
    if a.dry_run:
        print(f"Dry run: nothing written to the server. Review queue in {OUT / 'review.json'}")
    else:
        http("PUT", f"{base}/Binary/{REVIEW_ID}", json_binary(REVIEW_ID, doc))
        print(f"Published Binary/{REVIEW_ID} ({len(doc['pairs'])} pairs) and {OUT / 'review.json'}")
    print_summary(doc)
    return 0


def decide(doc, pid, status, when):
    """The review queue with one pair decided. Raises ValueError for an unknown pair or one linked automatically."""
    pair = next((p for p in doc.get("pairs", []) if p["pair_id"] == pid), None)
    if pair is None:
        raise ValueError(f"no pair {pid} in the review queue")
    if pair["status"] == "auto_link":
        raise ValueError(f"{pid} was linked automatically (score {pair['score']}); links are never removed here")
    if pair["status"] == "accepted" and status == "rejected":
        raise ValueError(f"{pid} was accepted and linked already; links are never removed here")
    doc = json.loads(json.dumps(doc))
    for p in doc["pairs"]:
        if p["pair_id"] == pid:
            p["status"], p["decided_at"] = status, when
    c = counts(doc["pairs"])
    c["no_match"], c["candidate_pairs"] = doc["counts"].get("no_match", 0), doc["counts"].get("candidate_pairs", len(doc["pairs"]))
    doc["counts"] = c
    return doc


def cmd_decide(a):
    base = a.base.rstrip("/")
    require_local(base)
    doc = read_binary(base, REVIEW_ID)
    if not doc:
        print(f"No Binary/{REVIEW_ID} on {base}. Run: python3 empi/match.py run", file=sys.stderr)
        return 1
    status = "accepted" if a.accept else "rejected"
    try:
        doc = decide(doc, a.pair_id, status, now_utc())
    except ValueError as e:
        print(f"Cannot decide: {e}", file=sys.stderr)
        return 1
    pair = next(p for p in doc["pairs"] if p["pair_id"] == a.pair_id)
    if status == "accepted":
        n = link_pair(base, pair["a"]["id"], pair["b"]["id"])
        doc["links"] = {"pairs_linked": len(to_link(doc["pairs"])), "patients_updated": n}
        print(f"Linked Patient/{pair['a']['id']} and Patient/{pair['b']['id']} ({n} updated)")
    doc["quality"] = quality(doc["pairs"], read_truth(base))
    http("PUT", f"{base}/Binary/{REVIEW_ID}", json_binary(REVIEW_ID, doc))
    save_local(doc)
    print(f"{a.pair_id} {status}: {pair['a']['name']} and {pair['b']['name']} (score {pair['score']})")
    return 0


def read_truth_file(path):
    """The pairs of a ground truth file written by pipeline.py empi-seed. Raises ValueError for anything else."""
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    pairs = doc.get("pairs") if isinstance(doc, dict) else None
    if not isinstance(pairs, list) or not all({"seed_id", "existing_id", "kind"} <= set(t) for t in pairs):
        raise ValueError(f"{path} is not an EMPI ground truth file (python3 hl7v2/pipeline.py empi-seed writes one)")
    return doc


def cmd_evaluate(a):
    base = a.base.rstrip("/")
    try:
        truth = read_truth_file(a.truth)
    except (OSError, ValueError) as e:
        print(f"Cannot read the truth: {e}", file=sys.stderr)
        return 1
    records = [record(p) for p in fetch_patients(base)]
    missing = {t["seed_id"] for t in truth["pairs"]} - {r["id"] for r in records}
    if missing:
        print(f"{len(missing)} test records of {a.truth} are not on {base}: run python3 hl7v2/pipeline.py empi-seed "
              f"--seed {truth.get('seed')} first", file=sys.stderr)
        return 1
    # decisions a person made stand, as in run; nothing is written
    previous = (read_binary(base, REVIEW_ID) or {}).get("pairs", [])
    q = quality(match_all(records, load_config(a.config), load_nicknames(), previous), truth["pairs"])
    print(f"Evaluated on {a.truth} (seed {truth.get('seed')}): {q['true_match_pairs']} true matches, "
          f"{q['hard_negative_pairs']} hard negatives")
    print(f"  precision {q['precision']} ({q['true_positives']} of {q['auto_linked']} automatic links are true matches)")
    print(f"  recall    {q['recall']} ({q['true_positives']} of {q['true_match_pairs']} true matches linked automatically)")
    print(f"  hard negatives linked automatically {q['hard_negatives_auto_linked']}, pairs in review {q['pairs_in_review']} "
          f"(true matches {q['true_matches_in_review']}, hard negatives {q['hard_negatives_in_review']})")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--base", default=DEFAULT_BASE)
    r.add_argument("--config", default=str(CONFIG))
    r.add_argument("--dry-run", action="store_true")
    r.set_defaults(func=cmd_run)
    d = sub.add_parser("decide")
    d.add_argument("pair_id")
    g = d.add_mutually_exclusive_group(required=True)
    g.add_argument("--accept", action="store_true")
    g.add_argument("--reject", action="store_true")
    d.add_argument("--base", default=DEFAULT_BASE)
    d.set_defaults(func=cmd_decide)
    e = sub.add_parser("evaluate")
    e.add_argument("--truth", required=True, help="a ground truth file from hl7v2/pipeline.py empi-seed")
    e.add_argument("--base", default=DEFAULT_BASE)
    e.add_argument("--config", default=str(CONFIG))
    e.set_defaults(func=cmd_evaluate)
    a = ap.parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
