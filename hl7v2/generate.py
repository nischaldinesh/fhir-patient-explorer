"""Generate a realistic, messy HL7 v2.5 feed for synthetic patients (fixed seed, same feed every run).

build() writes the original 25 patient feed. build_day() writes one day of live traffic on real dates,
continuing from a FeedState (who is in hospital, who has been here before), so the feed keeps arriving.

Every person, address and facility here is invented. The mess is deliberate and mirrors real interfaces:
  * lab tests arrive as free text or local codes ("GLU", "Blood sugar", "GLUCOSE, SERUM"), a few as LOINC
  * diagnoses arrive in retired ICD 9, sometimes dotted ("250.00"), sometimes ICD 10, once with no code
  * results that are not numbers ("<0.01", "Moderate") where a number is expected
  * non clinical traffic: acknowledgments (ACK), queries (QRY), and message types we do not handle
  * duplicate resends of the same message, and a few broken messages
"""
import random
from datetime import date, datetime, time, timedelta

import reference as ref

FACILITY = "RIVERSIDE^RIVERSIDE GENERAL HOSPITAL"
RECEIVER = "FHIRBRIDGE|FHIRBRIDGE"  # MSH-5 receiving application, MSH-6 receiving facility
SEG = "\r"

FIRST = ["Maya", "Liam", "Aisha", "Noah", "Priya", "Ethan", "Sofia", "Omar", "Grace", "Mateo", "Hana", "Lucas",
         "Zara", "Daniel", "Elena", "Kofi", "Ava", "Ravi", "Chloe", "Diego", "Mei", "Isaac", "Leila", "Owen", "Nina"]
LAST = ["Okafor", "Lindqvist", "Ramirez", "O'Brien", "Nakamura", "Haddad", "Kowalski", "Mensah", "Fischer",
        "Delacroix", "Patel", "Novak", "Achterberg", "Silva", "Brennan-Ross", "Yilmaz", "Moreau", "Kaur",
        "Abernethy", "Castillo", "Duarte", "Ivanova", "Lindgren", "Oyelaran", "Whitcombe"]
STREETS = ["Maple Ave", "Harbor St", "Elm St", "Ridge Rd", "Willow Ln", "Cedar Ct", "Mill St", "Orchard Way"]
CITIES = [("Lowell", "01852"), ("Quincy", "02169"), ("Salem", "01970"), ("Framingham", "01701"), ("Medford", "02155")]
FEMALE = {"Maya", "Aisha", "Priya", "Sofia", "Grace", "Hana", "Zara", "Elena", "Ava", "Chloe", "Mei", "Leila", "Nina"}

# (OBX-3 variants as sent by different senders, units, normal range, typical value range, decimals)
LABS = {
    "glucose": (["GLU^Glucose^L", "^Blood sugar", "GLUC^GLUCOSE, SERUM^L", "2345-7^Glucose [Mass/volume] in Serum or Plasma^LN"],
                "mg/dL", (70, 105), (72, 260), 0),
    "sodium": (["NA^Sodium^L", "^Na", "^SODIUM"], "mmol/L", (133, 145), (126, 146), 0),
    "potassium": (["K^Potassium^L", "^K+", "^POTASSIUM"], "mmol/L", (3.3, 5.1), (3.0, 5.8), 1),
    "creatinine": (["CREA^Creatinine^L", "^Creat", "^CREATININE"], "mg/dL", (0.5, 1.2), (0.6, 3.4), 2),
    "bun": (["BUN^Urea nitrogen^L", "^BUN"], "mg/dL", (6, 20), (8, 60), 0),
    "hemoglobin": (["HGB^Hemoglobin^L", "^Hgb", "^HEMOGLOBIN"], "g/dL", (12, 16), (7.5, 15.8), 1),
    "wbc": (["WBC^White blood cells^L", "^WBC count"], "10*3/uL", (4, 11), (3.2, 19.0), 1),
    "lactate": (["LACT^Lactate^L", "^Lactic acid"], "mmol/L", (0.5, 2.0), (0.7, 5.5), 1),
    "troponin": (["TROP^Troponin T^L"], "ng/mL", (0, 0.01), (0.0, 0.4), 2),
}
PANELS = {
    "BMP^Basic metabolic panel^L": ["glucose", "sodium", "potassium", "creatinine", "bun"],
    "CBC^Complete blood count^L": ["hemoglobin", "wbc"],
    "CARD^Cardiac markers^L": ["troponin", "lactate"],
}

# (code, text, coding system) as found in real ADT feeds
DIAGNOSES = [
    ("4019", "HYPERTENSION NOS", "I9"), ("25000", "DMII WO CMP NT ST UNCNTR", "I9"), ("2724", "HYPERLIPIDEMIA NEC/NOS", "I9"),
    ("4280", "CHF NOS", "I9"), ("51881", "ACUTE RESPIRATRY FAILURE", "I9"), ("5849", "ACUTE KIDNEY FAILURE NOS", "I9"),
    ("42731", "ATRIAL FIBRILLATION", "I9"), ("486", "PNEUMONIA, ORGANISM NOS", "I9"), ("250.00", "DIABETES TYPE II", "I9C"),
    ("I10", "Essential (primary) hypertension", "I10"), ("E119", "Type 2 diabetes mellitus without complications", "I10"),
    ("J189", "Pneumonia, unspecified organism", "I10"),
]


def segment(name: str, fields: dict) -> str:
    """Build a segment from {field number: value}, so positions never depend on counting pipes."""
    values = [""] * (max(fields) + 1)
    for n, v in fields.items():
        values[n] = str(v)
    return name + "|".join(values)


def pv1(visit, location="", attending="", admit="", discharge="", patient_class="I", admission_type="", prior=""):
    """PV1-2 patient class (I inpatient, E emergency), PV1-3 unit^room^bed, PV1-4 admission type (E emergency,
    U urgent, R elective), PV1-6 prior location (on a transfer)."""
    f = {1: "1", 2: patient_class, 19: visit}
    if location:
        f[3] = location
    if admission_type:
        f[4] = admission_type
    if prior:
        f[6] = prior
    if attending:
        f[7] = attending
    if admit:
        f[44] = admit
    if discharge:
        f[45] = discharge
    return segment("PV1", f)


def ts(d: datetime) -> str:
    return d.strftime("%Y%m%d%H%M%S")


class Feed:
    def __init__(self, seed=42):
        self.rnd = random.Random(seed)
        self.n = 0
        self.messages = []  # (datetime, label, text)

    def control_id(self):
        self.n += 1
        return f"RVG{self.n:06d}"

    def msh(self, when, mtype, app="EPICADT", cid=None):
        cid = cid or self.control_id()
        return f"MSH|^~\\&|{app}|{FACILITY}|{RECEIVER}|{ts(when)}||{mtype}|{cid}|P|2.5.1", cid

    def add(self, when, label, segments):
        self.messages.append((when, label, SEG.join(segments) + SEG))

    def ack(self, when, cid):
        m, _ = self.msh(when + timedelta(seconds=2), "ACK^A01^ACK", app="FHIRBRIDGE")
        self.add(when + timedelta(seconds=2), "ACK", [m, f"MSA|AA|{cid}"])


def patient(rnd, i):
    first, last = FIRST[i], LAST[i]
    sex = "F" if first in FEMALE else "M"
    born = datetime(1940, 1, 1) + timedelta(days=rnd.randint(0, 365 * 62))
    city, zip_ = CITIES[i % len(CITIES)]
    street = f"{rnd.randint(10, 990)} {STREETS[i % len(STREETS)]}"
    if i == 7:
        street += " \\T\\ Rear"  # escaped "&" inside a field
    return {"mrn": f"RV{100200 + i * 7}", "first": first, "last": last, "middle": rnd.choice(["A", "J", "M", ""]),
            "sex": sex, "dob": born.strftime("%Y%m%d"), "street": street, "city": city, "zip": zip_,
            "phone": f"(555)555-{1000 + i:04d}"}


def pid(p, mrn_variant=False):
    mrn = f"{p['mrn']}^^^RIVERSIDE^MR"
    name = f"{p['last']}^{p['first']}^{p['middle']}"
    return (f"PID|1||{mrn}||{name}||{p['dob']}|{p['sex']}|||{p['street']}^^{p['city']}^MA^{p['zip']}^USA"
            f"||{p['phone']}")


def obx(rnd, set_id, lab, when, value=None, performer=""):
    """One result. value fixes the number (used for a creatinine trend); otherwise it is random.
    performer fills OBX-23, the performing organization (the live feed only; the original feed has none)."""
    variants, unit, (lo, hi), (vmin, vmax), dec = LABS[lab]
    ident = rnd.choice(variants)
    v = round(rnd.uniform(vmin, vmax), dec) if value is None else round(value, dec)
    tail = "|" * 9 + performer if performer else ""  # OBX-15 to OBX-22 empty, then OBX-23
    if value is None and lab == "troponin" and rnd.random() < 0.6:
        return f"OBX|{set_id}|ST|{ident}||<0.01|{unit}|{lo}-{hi}|N|||F|||{ts(when)}{tail}"
    if value is None and lab == "creatinine" and rnd.random() < 0.08:
        return f"OBX|{set_id}|ST|{ident}||<0.3|{unit}|{lo}-{hi}|L|||F|||{ts(when)}{tail}"
    flag = "H" if v > hi else "L" if v < lo else "N"
    value = f"{v:.{dec}f}" if dec else str(int(v))
    status = "C" if rnd.random() < 0.03 else "F"  # occasional corrected result
    return f"OBX|{set_id}|NM|{ident}||{value}|{unit}|{lo}-{hi}|{flag}|||{status}|||{ts(when)}{tail}"


def build(seed=42, patients=25):
    feed = Feed(seed)
    rnd = feed.rnd
    start = datetime(2026, 3, 2, 7, 30)

    for i in range(patients):
        p = patient(rnd, i)
        admit = start + timedelta(days=rnd.randint(0, 20), hours=rnd.randint(0, 14), minutes=rnd.randint(0, 59))
        visit = f"V{70000 + i * 13}"

        # Admission with diagnoses
        m, cid = feed.msh(admit, "ADT^A01^ADT_A01")
        location = f"{rnd.choice(['MED', 'ICU', 'CARD'])}^{rnd.randint(200, 480)}^A"
        visit_pv1 = pv1(visit, location=location, attending="1234^Morgan^Dana^^^DR", admit=ts(admit))
        dg1 = []
        for k, (code, text, system) in enumerate(rnd.sample(DIAGNOSES, rnd.randint(1, 3)), start=1):
            dg1.append(f"DG1|{k}||{code}^{text}^{system}||{ts(admit)}|A")
        if i == 11:
            dg1.append(f"DG1|{len(dg1) + 1}||^Chest pain, unspecified||{ts(admit)}|A")  # free text, no code
        feed.add(admit, "ADT_A01", [m, f"EVN|A01|{ts(admit)}", pid(p), visit_pv1, *dg1])
        if rnd.random() < 0.6:
            feed.ack(admit, cid)

        # Lab results over the stay
        for d in range(rnd.randint(1, 4)):
            when = admit + timedelta(hours=6 + d * rnd.randint(10, 26))
            panels = rnd.sample(list(PANELS), rnd.randint(1, 3))
            m, cid = feed.msh(when + timedelta(minutes=45), "ORU^R01^ORU_R01", app="LABSYS")
            segs, set_id = [m, pid(p), pv1(visit)], 0
            for o, panel in enumerate(panels, start=1):
                segs.append(f"OBR|{o}||L{when:%y%m%d%H}{i:02d}{o}|{panel}|||{ts(when)}")
                for lab in PANELS[panel]:
                    set_id += 1
                    segs.append(obx(rnd, set_id, lab, when))
            feed.add(when + timedelta(minutes=45), "ORU_R01", segs)
            if rnd.random() < 0.5:
                feed.ack(when + timedelta(minutes=45), cid)

        # Nursing vitals, the "Body Height" case: free text, no code
        if i % 2 == 0:
            when = admit + timedelta(hours=1)
            m, _ = feed.msh(when, "ORU^R01^ORU_R01", app="NURSING")
            feed.add(when, "ORU_R01", [m, pid(p), pv1(visit),
                                       f"OBR|1||N{i:03d}|VS^Vital signs^L|||{ts(when)}",
                                       f"OBX|1|NM|^Body Height||{rnd.randint(150, 192)}|cm||||||F|||{ts(when)}",
                                       f"OBX|2|NM|^Body Weight||{round(rnd.uniform(48, 118), 1)}|kg||||||F|||{ts(when)}",
                                       f"OBX|3|ST|^Pain score||{rnd.choice(['None', 'Mild', 'Moderate'])}|||||||F|||{ts(when)}"])

        # Demographics update for some, discharge for most
        if i % 5 == 1:
            when = admit + timedelta(days=1)
            p["street"] = f"{rnd.randint(10, 990)} {rnd.choice(STREETS)}"
            m, _ = feed.msh(when, "ADT^A08^ADT_A01")
            feed.add(when, "ADT_A08", [m, f"EVN|A08|{ts(when)}", pid(p), pv1(visit)])
        if i % 4 != 3:
            when = admit + timedelta(days=rnd.randint(2, 9))
            m, _ = feed.msh(when, "ADT^A03^ADT_A03")
            feed.add(when, "ADT_A03", [m, f"EVN|A03|{ts(when)}", pid(p),
                                       pv1(visit, admit=ts(admit), discharge=ts(when))])

    # Non clinical and unsupported traffic
    for k in range(18):
        when = start + timedelta(days=rnd.randint(0, 25), minutes=rnd.randint(0, 1400))
        m, _ = feed.msh(when, "QRY^A19^QRY_A19", app="REGISTRY")
        feed.add(when, "QRY_A19", [m, f"QRD|{ts(when)}|R|I|Q{k:04d}|||1^RD|{rnd.choice(FIRST)}|DEM"])
    for k, mtype in enumerate(["MDM^T02^MDM_T02", "SIU^S12^SIU_S12", "MDM^T02^MDM_T02", "SIU^S12^SIU_S12"]):
        when = start + timedelta(days=3 + k * 4, hours=k)
        m, _ = feed.msh(when, mtype, app="OTHERSYS")
        feed.add(when, mtype.split("^")[0], [m, pid(patient(random.Random(k), k))])

    # Duplicate resends: the same message, byte for byte, a few minutes later
    clinical = [x for x in feed.messages if x[1] in ("ADT_A01", "ORU_R01")]
    for when, label, text in rnd.sample(clinical, 10):
        feed.messages.append((when + timedelta(minutes=rnd.randint(3, 40)), label, text))

    # Broken messages
    when = start + timedelta(days=12)
    m, _ = feed.msh(when, "ORU^R01^ORU_R01", app="LABSYS")
    feed.messages += [
        (when, "BROKEN", f"PID|1||RV999999^^^RIVERSIDE^MR||Lost^Header" + SEG),
        (when + timedelta(minutes=1), "BROKEN", f"MSH|^~\\&|LABSYS|{FACILITY}|{RECEIVER}|{ts(when)}|||RVGBAD01|P|2.5.1" + SEG),
        (when + timedelta(minutes=2), "BROKEN", SEG.join([m, "OBR|1||X1|BMP^Basic metabolic panel^L", "OBX|1|NM|GLU^Glucose^L||99|mg/dL"]) + SEG),
        (when + timedelta(minutes=3), "BROKEN", "MSH|^^^^|LABSYS|RIVERSIDE" + SEG),
    ]

    feed.messages.sort(key=lambda x: x[0])
    return feed.messages


# ---------- the live feed: one day at a time, on real dates ----------

CHRONIC = [("4019", "HYPERTENSION NOS", "I9"), ("25000", "DMII WO CMP NT ST UNCNTR", "I9"), ("2724", "HYPERLIPIDEMIA NEC/NOS", "I9"),
           ("42731", "ATRIAL FIBRILLATION", "I9"), ("250.00", "DIABETES TYPE II", "I9C"),
           ("I10", "Essential (primary) hypertension", "I10"), ("E119", "Type 2 diabetes mellitus without complications", "I10")]
ACUTE = [("4280", "CHF NOS", "I9"), ("51881", "ACUTE RESPIRATRY FAILURE", "I9"), ("486", "PNEUMONIA, ORGANISM NOS", "I9"),
         ("J189", "Pneumonia, unspecified organism", "I10"), ("42731", "ATRIAL FIBRILLATION", "I9")]
AKI_DX = ("5849", "ACUTE KIDNEY FAILURE NOS", "I9")
ED_DX = [("78650", "CHEST PAIN NOS", "I9"), ("R079", "Chest pain, unspecified", "I10"), ("R55", "Syncope and collapse", "I10"),
         ("J069", "Acute upper respiratory infection, unspecified", "I10"), ("R109", "Unspecified abdominal pain", "I10")]
ADMISSION_TYPES = [("E", 0.6), ("U", 0.2), ("R", 0.2)]  # PV1-4: about 60% of admissions are emergencies
UNIT_MIX = [("MED", 0.55), ("CARD", 0.25), ("ICU", 0.2)]    # the unit a new admission goes to

# The staff roster (reference.STAFF) as HL7 XCN values, for PV1-7 attending and ROL-4 role person
_XCN = {s["number"]: ref.xcn(s) for s in ref.STAFF}
HOSPITALISTS = [_XCN[n] for n in ref.staff("doctor", "Hospitalist")]
ED_PHYSICIANS = [_XCN[n] for n in ref.staff("doctor", "Emergency physician")]
CARDIOLOGISTS = [_XCN[n] for n in ref.staff("doctor", "Cardiologist")]
INTENSIVISTS = [_XCN[n] for n in ref.staff("doctor", "Intensivist")]
NURSES = [_XCN[n] for n in ref.staff("nurse")]
CARE_MANAGERS = [_XCN[n] for n in ref.staff("care_manager")]
ROLES = {"PN": "PN^Primary nurse^L", "CM": "CM^Care manager^L"}  # ROL-3, local codes
LAB_XON = f"Riverside Reference Laboratory^^^^^^^^^{ref.LAB_CODE}"  # OBX-23: name, then XON-10 identifier


def attending_for(rnd, unit):
    """The attending of record: intensivist in the ICU, cardiologist or hospitalist on the cardiac unit."""
    if unit == "ICU":
        return rnd.choice(INTENSIVISTS)
    if unit == "CARD":
        return rnd.choice(CARDIOLOGISTS + HOSPITALISTS)
    return rnd.choice(HOSPITALISTS)


class DayFeed(Feed):
    """Control IDs carry the date (RVG<yyyymmdd><seq>), so they never repeat across days."""

    def __init__(self, day: date, rnd):
        super().__init__()
        self.rnd, self.tag = rnd, day.strftime("%Y%m%d")

    def control_id(self):
        self.n += 1
        return f"RVG{self.tag}{self.n:04d}"


def parse_ts(v: str) -> datetime:
    return datetime.strptime(v, "%Y%m%d%H%M%S")


def live_patient(rnd, mrn_number):
    """A new invented patient with a few long term conditions they bring to every admission."""
    first, last = rnd.choice(FIRST), rnd.choice(LAST)
    city, zip_ = rnd.choice(CITIES)
    born = datetime(1935, 1, 1) + timedelta(days=rnd.randint(0, 365 * 70))
    return {"mrn": f"RV{mrn_number}", "first": first, "last": last, "middle": rnd.choice(["A", "J", "M", ""]),
            "sex": "F" if first in FEMALE else "M", "dob": born.strftime("%Y%m%d"),
            "street": f"{rnd.randint(10, 990)} {rnd.choice(STREETS)}", "city": city, "zip": zip_,
            "phone": f"(555){rnd.randint(200, 899)}-{mrn_number % 10000:04d}", "height": rnd.randint(150, 192),
            "comorbid": [list(d) for d in rnd.sample(CHRONIC, rnd.randint(0, 2))]}


def stay_pv1(adm, discharge="", prior=""):
    """The full PV1 of an admission. Every message about the visit repeats it, so whichever message
    is loaded last (PUT replaces the Encounter) still describes the whole visit."""
    return pv1(adm["visit"], location=adm["unit"], attending=adm["attending"], admit=adm["admit"],
               discharge=discharge, patient_class=adm["class"], admission_type=adm["admission_type"], prior=prior)


def rol(adm):
    """ROL segments: the care team members besides the attending (primary nurse, care manager at discharge)."""
    return [f"ROL|{adm['visit']}{code}|AD|{ROLES[code]}|{person}|{since}" for code, person, since in adm.get("roles", [])]


def weighted(rnd, choices):
    x, total = rnd.random(), 0.0
    for value, weight in choices:
        total += weight
        if x < total:
            return value
    return choices[-1][0]


class BedBoard:
    """Which bed is taken when, for one day. Open stays hold their bed until they leave; stays that ended today
    are kept as (start, end) intervals. A bed is offered only when it is free for the whole time asked for, so
    two patients never share a bed. Deliberately cautious: a bed held by an open stay is never offered, even for
    a time before that stay began."""

    def __init__(self, open_admissions):
        self.occupied = {a["bed"]: a["visit"] for a in open_admissions if a.get("bed")}
        self.closed = {}

    def free(self, bed, lo, hi=None):
        """Free from lo until hi, or from lo on when hi is None (an open stay)."""
        if bed in self.occupied:
            return False
        return all(ce <= lo if hi is None else (ce <= lo or cs >= hi) for cs, ce in self.closed.get(bed, []))

    def pick(self, rnd, units, lo, hi=None):
        """A free bed in the first unit that has one, or None."""
        for unit in units:
            free = [b for b, _ in ref.beds(unit) if self.free(b, lo, hi)]
            if free:
                return rnd.choice(free)
        return None

    def take(self, adm, bed, when):
        self.occupied[bed] = adm["visit"]
        adm.update(bed=bed, bed_since=ts(when), unit=ref.ALL_BEDS[bed])

    def release(self, adm, when):
        bed = adm.get("bed")
        if bed:
            self.occupied.pop(bed, None)
            self.closed.setdefault(bed, []).append((parse_ts(adm["bed_since"]), when))
            adm["bed"] = ""


def build_day(day: date, state, seed: int = 42):
    """One day of Riverside General traffic. Returns [(datetime, label, text)] sorted by time and updates
    state (roster, open admissions with their beds, discharges, carryover, last_day). Same state, day and
    seed: same messages."""
    if state.last_day and day.isoformat() <= state.last_day:
        raise ValueError(f"{day} is not after the last generated day {state.last_day}")
    rnd = random.Random(seed + day.toordinal())
    feed = DayFeed(day, rnd)
    start, end = datetime.combine(day, time()), datetime.combine(day, time()) + timedelta(days=1)
    roster = {p["mrn"]: p for p in state.roster}
    counters = {"visit": 0, "order": 0}
    board = BedBoard(state.open_admissions)

    def at(lo, hi):
        """A time between lo and hi (datetimes), to the minute."""
        span = max(int((hi - lo).total_seconds() // 60) - 1, 0)
        return lo + timedelta(minutes=rnd.randint(0, span))

    def next_visit():
        counters["visit"] += 1
        return f"V{feed.tag}{counters['visit']:02d}"

    def next_order(prefix):
        counters["order"] += 1
        return f"{prefix}{feed.tag}{counters['order']:03d}"

    def labs(adm, lo, hi):
        """1 or 2 blood draws between lo and hi, run by the reference lab (OBX-23). Creatinine follows a trend
        per stay; for a kidney injury stay it climbs 0.3 to 0.6 mg/dL per draw."""
        if hi - lo < timedelta(hours=1):
            return
        p = roster[adm["mrn"]]
        for when in sorted(at(lo, hi) for _ in range(rnd.randint(1, 2))):
            panels = rnd.sample(list(PANELS), rnd.randint(1, 3))
            if adm["aki"] and "BMP^Basic metabolic panel^L" not in panels:
                panels.insert(0, "BMP^Basic metabolic panel^L")
            sent = when + timedelta(minutes=45)
            m, cid = feed.msh(sent, "ORU^R01^ORU_R01", app="LABSYS")
            segs, set_id = [m, pid(p), stay_pv1(adm)], 0
            for o, panel in enumerate(panels, start=1):
                segs.append(f"OBR|{o}||{next_order('L')}|{panel}|||{ts(when)}")
                for lab in PANELS[panel]:
                    set_id += 1
                    value = None
                    if lab == "creatinine":
                        step = rnd.uniform(0.3, 0.6) if adm["aki"] else rnd.uniform(-0.1, 0.1)
                        adm["creatinine"] = min(max(adm["creatinine"] + step, 0.5), 4.5)
                        value = adm["creatinine"]
                    segs.append(obx(rnd, set_id, lab, when, value=value, performer=LAB_XON))
            feed.add(sent, "ORU_R01", segs)
            if rnd.random() < 0.5:
                feed.ack(sent, cid)

    def transfer(adm, units, when, bed=None):
        """ADT^A02: the patient moves to a free bed in the first of units that has one. False when none is free."""
        bed = bed or board.pick(rnd, units, when)
        if bed is None:
            return False
        prior = adm["unit"]
        board.release(adm, when)
        board.take(adm, bed, when)
        m, cid = feed.msh(when, "ADT^A02^ADT_A02")
        feed.add(when, "ADT_A02", [m, f"EVN|A02|{ts(when)}", pid(roster[adm["mrn"]]), stay_pv1(adm, prior=prior), *rol(adm)])
        if rnd.random() < 0.6:
            feed.ack(when, cid)
        return True

    def discharge(adm, when):
        m, _ = feed.msh(when, "ADT^A03^ADT_A03")
        feed.add(when, "ADT_A03", [m, f"EVN|A03|{ts(when)}", pid(roster[adm["mrn"]]), stay_pv1(adm, discharge=ts(when)),
                                   *rol(adm)])
        board.release(adm, when)

    # Resends an interface engine replayed after midnight: yesterday's messages, same control IDs
    for c in state.carryover:
        feed.messages.append((parse_ts(c["when"]), c["label"], c["text"]))
    state.carryover = []

    # Patients already in hospital: results, planned transfers, a few address updates, and discharge when the
    # stay is over
    still_open, discharged_today = [], []
    for adm in state.open_admissions:
        p = roster[adm["mrn"]]
        lo = start
        if not adm.get("bed"):
            # A stay from before beds were tracked: moved to a real bed early in the day, so it holds one
            lo = at(start + timedelta(minutes=5), start + timedelta(minutes=30))
            adm.setdefault("roles", [["PN", rnd.choice(NURSES), ts(lo)]])
            transfer(adm, [adm["unit"].split("^")[0], *ref.INPATIENT_UNITS], lo)
        planned = parse_ts(adm["discharge_at"])
        if planned.date() == day:
            leaving = planned
        elif planned < start:  # overdue after a gap in runs: discharged today
            leaving = at(start + timedelta(hours=8), start + timedelta(hours=18))
        else:
            leaving = None
        stop = (leaving - timedelta(hours=1)) if leaving else end
        moves = adm.get("moves", [])
        while moves and parse_ts(moves[0]["at"]) < end:
            move = moves.pop(0)
            when = max(parse_ts(move["at"]), lo + timedelta(minutes=10))
            if when >= stop:  # the discharge comes first
                moves.clear()
                break
            labs(adm, lo, when)
            if not transfer(adm, [move["unit"], *([] if move["unit"] == "ICU" else ["MED", "CARD"])], when):
                moves.clear()  # no bed free there: the patient stays put, and no return trip is needed
            lo = when
        labs(adm, lo, stop)
        if leaving is None and rnd.random() < 0.15:
            when = at(start + timedelta(hours=7), start + timedelta(hours=20))
            p["street"] = f"{rnd.randint(10, 990)} {rnd.choice(STREETS)}"
            m, _ = feed.msh(when, "ADT^A08^ADT_A01")
            feed.add(when, "ADT_A08", [m, f"EVN|A08|{ts(when)}", pid(p), stay_pv1(adm)])
        if leaving:
            # a care manager takes over at discharge and follows the patient up after they go home
            adm.setdefault("roles", []).append(["CM", rnd.choice(CARE_MANAGERS), ts(leaving)])
            discharge(adm, leaving)
            discharged_today.append({"visit": adm["visit"], "mrn": adm["mrn"], "discharged": ts(leaving)})
        else:
            still_open.append(adm)

    # New admissions: about 30% are patients who have been here before, preferring recent discharges.
    # About half of the emergency admissions wait in an ED bay first, then move up to their unit (ADT^A02).
    in_hospital = {a["mrn"] for a in still_open}
    recent = (day - timedelta(days=30)).strftime("%Y%m%d")
    new_admissions = []
    for _ in range(rnd.randint(2, 5)):
        taken = in_hospital | {a["mrn"] for a in new_admissions}
        known = sorted(m for m in roster if m not in taken)
        recently_out = sorted({d["mrn"] for d in state.discharges + discharged_today
                               if d["discharged"][:8] >= recent and d["mrn"] not in taken})
        if known and rnd.random() < 0.3:
            p = roster[rnd.choice(recently_out if recently_out and rnd.random() < 0.7 else known)]
        else:
            p = live_patient(rnd, state.next_mrn)
            state.next_mrn += 1
            roster[p["mrn"]] = p
            state.roster.append(p)
        admit = at(start, end - timedelta(hours=3))
        aki = rnd.random() < 0.2
        dx = [tuple(d) for d in p["comorbid"]] + rnd.sample(ACUTE, rnd.randint(1, 2))
        if aki and rnd.random() < 0.5:
            dx.append(AKI_DX)
        dx = list(dict.fromkeys(dx))  # the same code twice is one diagnosis
        unit = weighted(rnd, UNIT_MIX)
        units = [unit, *[u for u in ref.INPATIENT_UNITS if u != unit]]  # the next unit with room when full
        los = rnd.randint(2, 9)
        adm = {"visit": next_visit(), "mrn": p["mrn"], "admit": ts(admit), "class": "I",
               "admission_type": weighted(rnd, ADMISSION_TYPES), "unit": "", "bed": "",
               "attending": attending_for(rnd, unit), "roles": [["PN", rnd.choice(NURSES), ts(admit)]],
               "diagnoses": [list(d) for d in dx], "aki": aki, "creatinine": round(rnd.uniform(0.7, 1.1), 2),
               "discharge_at": ts(admit + timedelta(days=los, hours=rnd.randint(0, 6))), "moves": []}
        if unit != "ICU" and los >= 4 and rnd.random() < 0.15:  # gets worse: to the ICU for a day or two, then back
            out = admit + timedelta(days=rnd.randint(1, los - 3), hours=rnd.randint(0, 12))
            back = out + timedelta(days=rnd.randint(1, 2), hours=rnd.randint(0, 6))
            adm["moves"] = [{"at": ts(out), "unit": "ICU"}, {"at": ts(back), "unit": unit}]
        upstairs = ed_bed = ward_bed = None
        if adm["admission_type"] == "E" and rnd.random() < 0.5:
            upstairs = admit + timedelta(minutes=rnd.randint(60, 170))
            ed_bed = board.pick(rnd, ["ED"], admit, upstairs)
            ward_bed = board.pick(rnd, units, upstairs)
        if not (ed_bed and ward_bed):
            upstairs = ed_bed = None
            ward_bed = board.pick(rnd, units, admit)
            if ward_bed is None:
                raise RuntimeError(f"no free inpatient bed on {day}: Riverside General is full")
        board.take(adm, ed_bed or ward_bed, admit)
        m, cid = feed.msh(admit, "ADT^A01^ADT_A01")
        dg1 = [f"DG1|{k}||{code}^{text}^{system}||{ts(admit)}|A" for k, (code, text, system) in enumerate(dx, start=1)]
        feed.add(admit, "ADT_A01", [m, f"EVN|A01|{ts(admit)}", pid(p), stay_pv1(adm), *rol(adm), *dg1])
        if rnd.random() < 0.6:
            feed.ack(admit, cid)
        if rnd.random() < 0.5:  # nursing admission vitals, free text names as in the original feed
            when = admit + timedelta(hours=1)
            m, _ = feed.msh(when, "ORU^R01^ORU_R01", app="NURSING")
            feed.add(when, "ORU_R01", [m, pid(p), stay_pv1(adm), f"OBR|1||{next_order('N')}|VS^Vital signs^L|||{ts(when)}",
                                       f"OBX|1|NM|^Body Height||{p['height']}|cm||||||F|||{ts(when)}",
                                       f"OBX|2|NM|^Body Weight||{round(rnd.uniform(48, 118), 1)}|kg||||||F|||{ts(when)}"])
        lo = admit + timedelta(hours=2)
        if upstairs:
            labs(adm, lo, upstairs)
            transfer(adm, units, upstairs, bed=ward_bed)
            lo = max(lo, upstairs)
        labs(adm, lo, end)
        new_admissions.append(adm)

    # Emergency department visits that end the same day without admission: A04 register, A03 discharge
    ed_visits = 0
    for _ in range(rnd.randint(1, 3)):
        known = sorted(m for m in roster if m not in in_hospital and m not in {a["mrn"] for a in new_admissions})
        if known and rnd.random() < 0.3:
            p = roster[rnd.choice(known)]
        else:
            p = live_patient(rnd, state.next_mrn)
            state.next_mrn += 1
            roster[p["mrn"]] = p
            state.roster.append(p)
        arrive = at(start, end - timedelta(hours=7))
        leave = arrive + timedelta(hours=rnd.randint(2, 6), minutes=rnd.randint(0, 59))
        bed = board.pick(rnd, ["ED"], arrive, leave)
        if bed is None:  # every bay taken the whole time: this patient went to another hospital
            continue
        adm = {"visit": next_visit(), "mrn": p["mrn"], "admit": ts(arrive), "class": "E", "admission_type": "E",
               "unit": "", "attending": rnd.choice(ED_PHYSICIANS), "roles": [["PN", rnd.choice(NURSES), ts(arrive)]]}
        board.take(adm, bed, arrive)
        code, text, system = rnd.choice(ED_DX)
        m, cid = feed.msh(arrive, "ADT^A04^ADT_A01")
        feed.add(arrive, "ADT_A04", [m, f"EVN|A04|{ts(arrive)}", pid(p), stay_pv1(adm), *rol(adm),
                                     f"DG1|1||{code}^{text}^{system}||{ts(arrive)}|A"])
        if rnd.random() < 0.6:
            feed.ack(arrive, cid)
        discharge(adm, leave)
        ed_visits += 1

    # Non clinical and unsupported traffic
    for k in range(rnd.randint(1, 2)):
        when = at(start, end)
        m, _ = feed.msh(when, "QRY^A19^QRY_A19", app="REGISTRY")
        feed.add(when, "QRY_A19", [m, f"QRD|{ts(when)}|R|I|Q{feed.tag}{k:02d}|||1^RD|{rnd.choice(FIRST)}|DEM"])
    mtype = rnd.choice(["MDM^T02^MDM_T02", "SIU^S12^SIU_S12"])
    when = at(start, end)
    m, _ = feed.msh(when, mtype, app="OTHERSYS")
    feed.add(when, mtype.split("^")[0], [m, pid(roster[rnd.choice(sorted(roster))])])

    # Duplicate resends (about 3% of clinical messages); one that lands after midnight arrives tomorrow
    clinical = [x for x in feed.messages
                if x[1] in ("ADT_A01", "ADT_A02", "ADT_A03", "ADT_A04", "ADT_A08", "ORU_R01") and x[0] >= start]
    for when, label, text in [x for x in clinical if rnd.random() < 0.03]:
        again = when + timedelta(minutes=rnd.randint(3, 40))
        if again < end:
            feed.messages.append((again, label, text))
        else:
            state.carryover.append({"when": ts(again), "label": label, "text": text})
    if clinical and rnd.random() < 0.1:  # the engine replays part of its queue after an overnight restart
        when, label, text = rnd.choice(clinical)
        state.carryover.append({"when": ts(end + timedelta(minutes=rnd.randint(5, 50))), "label": label, "text": text})

    # About one broken message every few days
    if rnd.random() < 0.3:
        when = at(start, end)
        m, _ = feed.msh(when, "ORU^R01^ORU_R01", app="LABSYS")
        feed.messages.append((when, "BROKEN", rnd.choice([
            "PID|1||RV999999^^^RIVERSIDE^MR||Lost^Header" + SEG,
            f"MSH|^~\\&|LABSYS|{FACILITY}|{RECEIVER}|{ts(when)}|||RVGBAD{feed.tag}|P|2.5.1" + SEG,
            SEG.join([m, "OBR|1||X1|BMP^Basic metabolic panel^L", "OBX|1|NM|GLU^Glucose^L||99|mg/dL"]) + SEG,
            "MSH|^^^^|LABSYS|RIVERSIDE" + SEG,
        ])))

    state.open_admissions = still_open + new_admissions
    state.discharges += discharged_today
    state.prune_discharges(day)
    state.last_day = day.isoformat()
    feed.messages.sort(key=lambda x: x[0])
    return feed.messages


def occupied_beds(state):
    """Bed ids held by the patients in hospital, for the bed status on the Location resources."""
    return {a["bed"] for a in state.open_admissions if a.get("bed")}


def msh_day(text):
    """The date of MSH-7 (when the message was first sent) as YYYY-MM-DD, or None for a message without one."""
    fields = text.split(SEG, 1)[0].split("|")
    stamp = fields[6] if fields[0] == "MSH" and len(fields) > 6 else ""
    return f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}" if len(stamp) >= 8 and stamp[:8].isdigit() else None


def day_summary(messages, day=None):
    """Counts the run history needs from the generated traffic (resends excluded). Discharges are
    inpatient discharges; the same day ED discharges are counted under ed_visits.

    With day, only messages first sent that day count: a resend of yesterday's message that the engine
    replays after midnight is byte identical, so its MSH-7 still names yesterday, where it was counted."""
    seen, out = set(), {"new_admissions": 0, "discharges": 0, "ed_visits": 0, "transfers": 0}
    day = day.isoformat() if hasattr(day, "isoformat") else day
    for _, label, text in messages:
        if text in seen or (day and msh_day(text) != day):
            continue
        seen.add(text)
        if label == "ADT_A01":
            out["new_admissions"] += 1
        elif label == "ADT_A03" and f"{SEG}PV1|1|I|" in text:
            out["discharges"] += 1
        elif label == "ADT_A04":
            out["ed_visits"] += 1
        elif label == "ADT_A02":
            out["transfers"] += 1
    return out
