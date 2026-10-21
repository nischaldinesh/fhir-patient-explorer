"""The EMPI test set (demo): ADT^A04 registrations at Riverside General of people the Synthea archive already holds.

20 true matches are copies of Synthea patients with the variations real registrations have: a nickname, a typo in the
last name, no middle name, a new address, two birth date digits swapped, a new phone number, and one person who moved
and changed phone. 10 hard negatives are different people with the same first and last name as a Synthea patient:
born the same year elsewhere, born the same year in the same ZIP code, or born the same day but of the other sex.

Fixed seed: the same Synthea patients always give the same messages and the same ground truth. MRNs RV300001 to
RV300030, in shuffled order, so the MRN does not give away which records match. The live daily feed and the original
25 patient feed are not touched.

Another seed gives a held out set for evaluation: its own MRNs (mrn_start), its own control IDs, and none of the
Synthea patients the tuning set (seed 42) already uses, so the two sets share no person.
"""
import random
from datetime import date, datetime, timedelta

from generate import CITIES, SEG, STREETS, ts
from to_fhir import fhir_id

MRN_START = 300001
DEFAULT_SEED = 42
SENT = datetime(2026, 10, 1, 8, 0)
FACILITY = "RIVERSIDE^RIVERSIDE GENERAL HOSPITAL"
RECEIVER = "FHIRBRIDGE|FHIRBRIDGE"

# The 20 true matches: the variation(s) each copy carries
MATCH_PLAN = [["nickname"]] * 4 + [["typo"]] * 3 + [["missing_middle"]] * 3 + [["new_address"]] * 4 \
    + [["transposed_birth_date"]] * 3 + [["different_phone"]] * 2 + [["new_address", "different_phone"]]
# The 10 hard negatives: same first and last name, another person
NEGATIVE_PLAN = ["same_year_elsewhere"] * 5 + ["same_year_same_zip"] * 3 + ["same_birth_date_other_sex"] * 2
UNISEX = {"jody", "rosario", "angel", "jesse", "kerry", "blake", "riley", "dallas", "carey", "lindsay", "ivory",
          "teri", "jammie", "dawn", "santa"}


def _valid(y, m, d):
    try:
        date(int(y), int(m), int(d))
        return True
    except ValueError:
        return False


def transpositions(birth):
    """Every other valid date that differs from birth (YYYY-MM-DD) by two neighbouring digits of the month and day
    swapped, or by day and month swapped, in a stable order. The year is kept: blocking compares birth years, so a
    swapped year is a slip this matcher cannot see (see the README)."""
    digits = birth.replace("-", "")
    out = []
    for i in range(4, len(digits) - 1):
        if digits[i] != digits[i + 1]:
            s = digits[:i] + digits[i + 1] + digits[i] + digits[i + 2:]
            if _valid(s[:4], s[4:6], s[6:]) and s[:4] >= "1900":
                out.append(f"{s[:4]}-{s[4:6]}-{s[6:]}")
    y, m, d = birth.split("-")
    if m != d and _valid(y, d, m):
        out.append(f"{y}-{d}-{m}")
    return sorted(set(out) - {birth})


def typo(rnd, name):
    """One keyboard slip in a name: two neighbouring letters swapped, a letter doubled or one dropped."""
    for _ in range(20):
        i = rnd.randint(1, len(name) - 2)
        kind = rnd.choice(["swap", "double", "drop"])
        if kind == "swap":
            out = name[:i] + name[i + 1] + name[i] + name[i + 2:]
        elif kind == "double":
            out = name[:i] + name[i] + name[i:]
        else:
            out = name[:i] + name[i + 1:]
        if out.lower() != name.lower():
            return out
    return name + "e"


def hl7_phone(digits):
    return f"({digits[:3]}){digits[3:6]}-{digits[6:]}"


def new_phone(rnd, old=""):
    while True:
        p = f"555{rnd.randint(200, 989)}{rnd.randint(1000, 9999)}"
        if p != old:
            return p


def new_place(rnd, old_zip=""):
    """A street and town the person did not live in before."""
    city, zip_ = rnd.choice([c for c in CITIES if c[1] != old_zip])
    return f"{rnd.randint(10, 990)} {rnd.choice(STREETS)}", city, zip_


def eligible(records):
    """Synthea patients a test record can be made from: living, with a name, full birth date, phone and address."""
    return sorted((r for r in records if r["comparable"] and r["source"] == "synthea" and r["phone"] and r["address"]
                   and not r.get("deceased")), key=lambda r: r["id"])


def person(r):
    """A Synthea record as the fields an HL7 v2 PID segment carries."""
    return {"first": r["first"], "middle": r["middle"], "last": r["last"], "sex": "F" if r["sex"] == "female" else "M",
            "dob": r["birth_date"], "street": r["address"], "city": r["city"], "zip": r["postal_code"],
            "phone": r["phone"]}


def make_copy(rnd, r, variations, nicknames):
    """The same person, registered again with the given variations. Returns (fields, note)."""
    p, notes = person(r), []
    for v in variations:
        if v == "nickname":
            p["first"] = rnd.choice(sorted(nicknames[r["first"].lower()])).capitalize()
            notes.append(f"{p['first']} for {r['first']}")
        elif v == "typo":
            p["last"] = typo(rnd, r["last"])
            notes.append(f"{p['last']} for {r['last']}")
        elif v == "missing_middle":
            p["middle"] = ""
            notes.append(f"no middle name ({r['middle']})")
        elif v == "new_address":
            p["street"], p["city"], p["zip"] = new_place(rnd, r["postal_code"])
            notes.append(f"moved to {p['city']}")
        elif v == "transposed_birth_date":
            p["dob"] = rnd.choice(transpositions(r["birth_date"]))
            notes.append(f"born {p['dob']} for {r['birth_date']}")
        elif v == "different_phone":
            p["phone"] = new_phone(rnd, r["phone"])
            notes.append("new phone number")
    return p, "; ".join(notes)


def make_negative(rnd, r, kind):
    """Another person with the same first and last name. Returns (fields, note)."""
    p = person(r)
    year = int(r["birth_date"][:4])
    near = set(transpositions(r["birth_date"])) | {r["birth_date"]}
    p["middle"] = rnd.choice(["", "A", "J", "M", "R"])
    if r["middle"] and p["middle"] == r["middle"][0]:
        p["middle"] = ""
    p["phone"] = new_phone(rnd, r["phone"])
    if kind == "same_birth_date_other_sex":
        p["sex"] = "M" if p["sex"] == "F" else "F"
        p["street"], p["city"], p["zip"] = new_place(rnd, r["postal_code"])
        return p, f"born the same day, {'male' if p['sex'] == 'M' else 'female'}"
    while True:
        born = date(year, 1, 1) + timedelta(days=rnd.randint(0, 364))
        if born.isoformat() not in near and born.year == year:
            break
    p["dob"] = born.isoformat()
    if kind == "same_year_same_zip" and r["postal_code"]:
        p["street"] = f"{rnd.randint(10, 990)} {rnd.choice(STREETS)}"
        return p, f"born {p['dob']}, same ZIP code"
    p["street"], p["city"], p["zip"] = new_place(rnd, r["postal_code"])
    return p, f"born {p['dob']}, lives in {p['city']}"


def can_vary(r, variations, nicknames):
    for v in variations:
        if v == "nickname" and r["first"].lower() not in nicknames:
            return False
        if v == "typo" and len(r["last"]) < 4:
            return False
        if v == "missing_middle" and not r["middle"]:
            return False
        if v == "transposed_birth_date" and not transpositions(r["birth_date"]):
            return False
    return True


def can_negate(r, kind):
    if kind == "same_birth_date_other_sex":
        return r["first"].lower() in UNISEX
    if kind == "same_year_same_zip":
        return bool(r["postal_code"])
    return True


def pid_segment(mrn, p):
    dob = p["dob"].replace("-", "")
    return (f"PID|1||{mrn}^^^RIVERSIDE^MR||{p['last']}^{p['first']}^{p['middle']}||{dob}|{p['sex']}|||"
            f"{p['street']}^^{p['city']}^MA^{p['zip']}^USA||{hl7_phone(p['phone'])}")


def mrn_start(seed):
    """RV300001 for the tuning set (seed 42). A held out seed gets RV3<seed mod 900 + 100><nn>, from RV310001 up,
    never overlapping the tuning set."""
    return MRN_START if seed == DEFAULT_SEED else 300001 + 100 * (seed % 900 + 100)


def control_id(seed, k):
    """RVGEMPI0001 for the tuning set, RVGEMPIS7N0001 for seed 7: a held out set never repeats a control ID."""
    return f"RVGEMPI{k + 1:04d}" if seed == DEFAULT_SEED else f"RVGEMPIS{seed}N{k + 1:04d}"


def build_seed(records, nicknames, seed=DEFAULT_SEED, exclude=frozenset()):
    """ADT^A04 messages [(datetime, label, text)] and the ground truth, from match.record() dicts of the server's
    patients. exclude: patient ids not to draw from (the tuning set's, for a held out set). Raises ValueError when
    there are not enough Synthea patients to draw from."""
    rnd = random.Random(seed)
    pool = [r for r in eligible(records) if r["id"] not in exclude]
    rnd.shuffle(pool)
    planned = []

    def take(ok):
        for i, r in enumerate(pool):
            if ok(r):
                return pool.pop(i)
        raise ValueError("not enough Synthea patients for the EMPI test set (load Synthea first)")

    # the most demanding picks first, so they are not used up
    for variations in sorted(MATCH_PLAN, key=lambda v: v != ["nickname"]):
        r = take(lambda x: can_vary(x, variations, nicknames))
        planned.append(("match", r, variations))
    for kind in sorted(NEGATIVE_PLAN, key=lambda k: k != "same_birth_date_other_sex"):
        r = take(lambda x: can_negate(x, kind))
        planned.append(("hard_negative", r, [kind]))
    rnd.shuffle(planned)

    messages, pairs = [], []
    for k, (kind, r, variations) in enumerate(planned):
        mrn = f"RV{mrn_start(seed) + k}"
        p, note = make_copy(rnd, r, variations, nicknames) if kind == "match" else make_negative(rnd, r, variations[0])
        when = SENT + timedelta(minutes=17 * k)
        msh = f"MSH|^~\\&|EPICADT|{FACILITY}|{RECEIVER}|{ts(when)}||ADT^A04^ADT_A01|{control_id(seed, k)}|P|2.5.1"
        messages.append((when, "ADT_A04", SEG.join([msh, f"EVN|A04|{ts(when)}", pid_segment(mrn, p)]) + SEG))
        pairs.append({"seed_id": fhir_id("RIVERSIDE", mrn), "seed_mrn": mrn, "existing_id": r["id"],
                      "existing_source": r["source"], "kind": kind, "variations": variations, "note": note})
    truth = {"description": "EMPI test set (demo): which Riverside General registrations are Synthea patients",
             "seed": seed, "pairs": pairs}
    return messages, truth
