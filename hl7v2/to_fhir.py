"""Map routed HL7 v2 messages to US Core FHIR R4 resources.

The feed is replayed in order into a current state (one Patient per MRN, one Encounter per visit,
one Condition per visit and diagnosis, one Observation per result), then loaded with PUT by id.
Ids come from source identifiers, so replaying the same feed twice changes nothing on the server.

  PID -> Patient      PV1 -> Encounter      DG1 -> Condition      OBR/OBX -> Observation
  PV1-2 patient class -> Encounter.class (v3 ActCode), PV1-4 admission type -> Encounter.priority (v3 ActPriority).
  ADT^A08 updates the Patient, ADT^A03 closes the Encounter.

The "who and where" (see reference.py for the hospital's master data):
  PV1-3 unit^room^bed -> Encounter.location: the unit and the bed, each with status and period. Only ADT^A02
        moves a patient: it completes the current location and adds the new one. Places that are not beds of
        the hospital (the original feed) stay display text, as before.
  PV1-7 attending     -> Encounter.participant type ATND, pointing at the Practitioner.
  PV1-7 and ROL       -> CareTeam (attending, primary nurse, care manager from discharge) for the visit, linked
                         with CareTeam.encounter (R4 has no Encounter.careTeam). Members are PractitionerRoles.
  OBX-23              -> Observation.performer, the lab Organization.

A visit can span several days of the live feed, so the pipeline seeds the mapper with the Encounter and CareTeam
the server already holds (seed()); later messages are merged into them instead of starting over.

Everything the mapper could not do cleanly is counted in `stats` with a reason.
"""
import copy
import csv
import hashlib
import re
from collections import Counter
from pathlib import Path

import reference as rf

HERE = Path(__file__).resolve().parent
REPO = "https://github.com/nischaldinesh/fhir-patient-explorer"
SOURCE_TAG = {"system": f"{REPO}/CodeSystem/source", "code": "hl7v2", "display": "HL7 v2 feed"}
REVIEW_TAG = {"system": f"{REPO}/CodeSystem/review", "code": "needs-review", "display": "Needs coder review"}
MRN_SYSTEM = "https://riverside.example.org/fhir/sid/mrn"
VISIT_SYSTEM = "https://riverside.example.org/fhir/sid/visit"
LOCAL_LAB_SYSTEM = "https://riverside.example.org/fhir/CodeSystem/lab"
LOINC = "http://loinc.org"
UCUM = "http://unitsofmeasure.org"
ICD9CM = "http://hl7.org/fhir/sid/icd-9-cm"
ICD10CM = "http://hl7.org/fhir/sid/icd-10-cm"
US_CORE = "http://hl7.org/fhir/us/core/StructureDefinition"

GENDER = {"M": "male", "F": "female", "O": "other", "U": "unknown", "A": "other", "N": "unknown"}
PATIENT_CLASS = {"I": ("IMP", "inpatient encounter"), "O": ("AMB", "ambulatory"), "E": ("EMER", "emergency")}
ADMISSION_TYPE = {"E": ("EM", "emergency"), "U": ("UR", "urgent"), "R": ("EL", "elective")}  # PV1-4 -> v3 ActPriority
ENCOUNTER_TYPE = {"EMER": "Emergency department visit"}  # anything else stays "Hospital admission"
OBS_STATUS = {"F": "final", "C": "corrected", "P": "preliminary", "X": "cancelled", "R": "registered"}
INTERPRETATION = {"H": ("H", "High"), "L": ("L", "Low"), "N": ("N", "Normal"), "HH": ("HH", "Critical high"),
                  "LL": ("LL", "Critical low"), "A": ("A", "Abnormal")}
COMPARATOR = re.compile(r"^\s*(<=|>=|<|>)\s*(-?\d+(?:\.\d+)?)\s*$")
UCUM_UNITS = {"mg/dL", "mmol/L", "g/dL", "10*3/uL", "ng/mL", "cm", "kg", "%", "mEq/L"}
ATTENDER = {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType", "code": "ATND",
                        "display": "attender"}]}
CARE_TEAM_ROLE = "https://riverside.example.org/fhir/CodeSystem/care-team-role"
# ROL-3 (and PV1-7, the attending) -> CareTeam participant role
CARE_TEAM_ROLES = {"ATND": ("attending", "Attending physician"), "PN": ("primary-nurse", "Primary nurse"),
                   "CM": ("care-manager", "Care manager")}
ENCOUNTER_FOCUSED = {"coding": [{"system": "http://loinc.org", "code": "LA27976-2", "display": "Encounter-focused care team"}]}


# ---------- small helpers ----------

def fhir_id(*parts):
    """Lowercase, [a-z0-9-] only, at most 64 characters (FHIR id rules)."""
    raw = "-".join(str(p) for p in parts if p)
    clean = re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")
    if len(clean) > 64:
        clean = clean[:55] + "-" + hashlib.sha1(raw.encode()).hexdigest()[:8]
    return clean


def hl7_datetime(v):
    """20260304081100 -> 2026-03-04T08:11:00 ; 19900725 -> 1990-07-25. Time zone is not sent, so none is invented."""
    v = (v or "").strip()
    if len(v) >= 12 and v[:12].isdigit():
        sec = v[12:14] if len(v) >= 14 else "00"
        return f"{v[:4]}-{v[4:6]}-{v[6:8]}T{v[8:10]}:{v[10:12]}:{sec}"
    if len(v) >= 8 and v[:8].isdigit():
        return f"{v[:4]}-{v[4:6]}-{v[6:8]}"
    return ""


def normalize_name(text):
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", (text or "").lower())).strip()


def load_synonyms(path=HERE / "lab_synonyms.csv"):
    by_code, by_text = {}, {}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            target = by_code if r["match_type"] == "code" else by_text
            key = r["match"].upper() if r["match_type"] == "code" else normalize_name(r["match"])
            target[key] = {"loinc": r["loinc"], "display": r["loinc_display"], "note": r["note"]}
    return by_code, by_text


def match_lab(code, text, system, synonyms):
    """Return (loinc mapping or None, how it matched). Order: LOINC at source, local code, text synonym."""
    by_code, by_text = synonyms
    if system == "LN" and code:
        return {"loinc": code, "display": text, "note": ""}, "loinc_at_source"
    if code and code.upper() in by_code:
        return by_code[code.upper()], "local_code"
    if normalize_name(text) in by_text:
        return by_text[normalize_name(text)], "text_synonym"
    return None, "unmapped"


def parse_range(rng):
    m = re.match(r"^\s*(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*$", rng or "")
    return (float(m.group(1)), float(m.group(2))) if m else None


def number(v):
    f = float(v)
    return int(f) if f.is_integer() and "." not in v else f


def quantity(value, unit):
    q = {"value": value, "unit": unit}
    if unit in UCUM_UNITS:
        q.update(system=UCUM, code=unit)
    return q


def ref(resource_type, rid):
    return {"reference": f"{resource_type}/{rid}"}


# ---------- the mapper ----------

class FeedMapper:
    def __init__(self, translate=None, synonyms=None):
        """translate(icd9_code) -> list of {'code','display','equivalence'} or None when unavailable."""
        self.translate = translate
        self.synonyms = synonyms or load_synonyms()
        self.patients, self.encounters, self.conditions, self.observations = {}, {}, {}, {}
        self.care_teams = {}
        self.seeded = {}  # (resource type, id) -> what the server already holds, merged on first use
        self.stats = Counter()
        self.lab_matches = Counter()
        self.unmapped_labs = Counter()
        self.dx_outcomes = Counter()
        self.review = []

    # PID -> Patient
    def patient(self, pid):
        mrn, authority = pid.get(3, 1), pid.get(3, 4) or "RIVERSIDE"
        pid_id = fhir_id(authority, mrn)
        name = {"use": "official", "family": pid.get(5, 1), "given": [g for g in (pid.get(5, 2), pid.get(5, 3)) if g]}
        p = {
            "resourceType": "Patient", "id": pid_id,
            "meta": {"profile": [f"{US_CORE}/us-core-patient"], "tag": [SOURCE_TAG]},
            "identifier": [{"use": "usual", "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0203", "code": "MR"}],
                                                      "text": "Medical record number"},
                            "system": MRN_SYSTEM, "value": mrn, "assigner": {"display": authority}}],
            "name": [name],
            "gender": GENDER.get(pid.get(8).upper(), "unknown"),
        }
        if hl7_datetime(pid.get(7)):
            p["birthDate"] = hl7_datetime(pid.get(7))[:10]
        if pid.get(11, 1):
            p["address"] = [{k: v for k, v in {"line": [pid.get(11, 1)], "city": pid.get(11, 3), "state": pid.get(11, 4),
                                               "postalCode": pid.get(11, 5), "country": pid.get(11, 6)}.items() if v}]
        if pid.get(13):
            p["telecom"] = [{"system": "phone", "value": pid.get(13), "use": "home"}]
        return p

    def seed(self, resources):
        """Encounters and CareTeams the server already holds for visits still open, so a transfer or discharge
        today is merged into what earlier days recorded. Only resources a message touches are loaded again."""
        for r in resources:
            r = copy.deepcopy(r)
            meta = r.get("meta", {})
            for k in ("versionId", "lastUpdated", "source"):
                meta.pop(k, None)
            self.seeded[(r["resourceType"], r["id"])] = r

    def _current(self, store, resource_type, rid):
        if rid not in store and (resource_type, rid) in self.seeded:
            store[rid] = self.seeded.pop((resource_type, rid))
        return store.get(rid)

    # PV1-3 -> Encounter.location
    def locate(self, e, pv1, when, moved, admitted):
        """Unit and bed entries for a bed of the hospital, display text for any other place. Only an ADT^A02
        moves the patient: it completes the current entries and adds the new ones. Any other message that names
        a different place does not move anyone (counted in stats)."""
        unit, room, bed = pv1.get(3, 1), pv1.get(3, 2), pv1.get(3, 3)
        if not unit:
            return
        text = " ".join(x for x in (unit, room, bed) if x)
        bid = rf.bed_id(unit, room, bed)
        entries = e.get("location", [])
        tracked = any("status" in x for x in entries)
        if bid is None and not tracked and not moved:
            e["location"] = [{"location": {"display": text}}]  # never had a bed: the place as text, as before
            return
        current = [x for x in entries if x.get("status", "active") == "active" and "end" not in x.get("period", {})]
        target = f"Location/{bid}" if bid else None
        if current and any(x["location"].get("reference") == target and target for x in current):
            return  # already there: a later message about the same stay, or a rerun
        if current and not moved:
            self.stats["location_change_without_transfer_ignored"] += 1
            return
        if entries and not current and not moved:
            return  # the stay is over: a late resend places nobody
        for x in current:
            x["status"] = "completed"
            x.setdefault("period", {})["end"] = when
        period = {"start": when} if (moved or admitted) and when else {}
        if bid:
            u = unit.upper()
            new = [{"location": {"reference": f"Location/{rf.unit_id(u)}", "display": rf.UNITS[u][0]},
                    "status": "active", "physicalType": {"coding": [{"system": rf.PHYSICAL_TYPE, "code": "wa", "display": "Ward"}]}},
                   {"location": {"reference": target, "display": rf.bed_label(u, room, bed)},
                    "status": "active", "physicalType": {"coding": [{"system": rf.PHYSICAL_TYPE, "code": "bd", "display": "Bed"}]}}]
        else:
            new = [{"location": {"display": text}, "status": "active"}]
        for x in new:
            if period:
                x["period"] = dict(period)
        e["location"] = entries + new
        if moved and current:
            self.stats["transfers_applied"] += 1

    # PV1 -> Encounter (merged with what earlier messages said about the same visit)
    def encounter(self, pv1, patient_id, msg_time, discharged=False, moved=False, admitted=False, event_time=""):
        visit = pv1.get(19)
        if not visit:
            self.stats["encounter_without_visit_number"] += 1
            return None
        eid = fhir_id("rv", visit)
        e = self._current(self.encounters, "Encounter", eid) or {
            "resourceType": "Encounter", "id": eid,
            "meta": {"profile": [f"{US_CORE}/us-core-encounter"], "tag": [SOURCE_TAG]},
            "identifier": [{"system": VISIT_SYSTEM, "value": visit}],
            "status": "in-progress",
            "type": [{"text": "Hospital admission"}],
            "subject": ref("Patient", patient_id),
            "period": {},
        }
        cls = PATIENT_CLASS.get(pv1.get(2))
        if cls:
            e["class"] = {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": cls[0], "display": cls[1]}
            if cls[0] in ENCOUNTER_TYPE:
                e["type"] = [{"text": ENCOUNTER_TYPE[cls[0]]}]
        priority = ADMISSION_TYPE.get(pv1.get(4).upper())
        if priority:
            e["priority"] = {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-ActPriority",
                                         "code": priority[0], "display": priority[1]}]}
        start = hl7_datetime(pv1.get(44)) or e["period"].get("start") or hl7_datetime(msg_time)
        e["period"]["start"] = start
        when = event_time or hl7_datetime(msg_time)
        self.locate(e, pv1, start if admitted else when, moved, admitted)
        if pv1.get(7, 2):
            s = rf.STAFF_BY_NUMBER.get(pv1.get(7, 1))
            individual = ({"reference": f"Practitioner/{rf.practitioner_id(s['number'])}", "display": rf.display_name(s)}
                          if s else {"display": f"Dr {pv1.get(7, 3)} {pv1.get(7, 2)}".strip()})
            if not s:
                self.stats["attending_not_on_staff_list"] += 1
            e["participant"] = [{"type": [ATTENDER], "individual": individual}]
        if discharged:
            e["status"] = "finished"
            e["period"]["end"] = hl7_datetime(pv1.get(45)) or hl7_datetime(msg_time)
            for x in e.get("location", []):
                if x.get("status") == "active":
                    x["status"] = "completed"
                    x.setdefault("period", {})["end"] = e["period"]["end"]
        self.encounters[eid] = e
        return eid

    # PV1-7 and ROL -> CareTeam for the visit
    def care_team(self, msg, encounter_id, patient_id, discharged=False):
        e = self.encounters[encounter_id]
        members = []  # (role code, staff number, name as sent, since)
        att = (e.get("participant") or [{}])[0].get("individual", {})
        if att:
            number = att.get("reference", "").rsplit("riverside-pract-", 1)[-1] if att.get("reference") else ""
            members.append(("ATND", number, att.get("display", ""), e["period"].get("start")))
        for rol in msg.all("ROL"):
            code = rol.get(3, 1).upper()
            if code in CARE_TEAM_ROLES and (rol.get(4, 1) or rol.get(4, 2)):
                members.append((code, rol.get(4, 1), f"{rol.get(4, 3)} {rol.get(4, 2)}".strip(), hl7_datetime(rol.get(5))))
        ct_id = fhir_id(encounter_id, "careteam")
        ct = self._current(self.care_teams, "CareTeam", ct_id)
        if not members and not ct:
            return
        ct = ct or {
            "resourceType": "CareTeam", "id": ct_id,
            "meta": {"profile": [f"{US_CORE}/us-core-careteam"], "tag": [SOURCE_TAG]},
            "status": "active", "category": [ENCOUNTER_FOCUSED],
            "name": f"Care team for visit {e['identifier'][0]['value']}",
            "subject": ref("Patient", patient_id), "encounter": ref("Encounter", encounter_id),
            "period": {k: v for k, v in {"start": e["period"].get("start")}.items() if v},
            "participant": [],
            "managingOrganization": [{"reference": f"Organization/{rf.HOSPITAL_ID}", "display": "Riverside General Hospital"}],
        }
        for code, number, name, since in members:
            role_code, role_display = CARE_TEAM_ROLES[code]
            s = rf.STAFF_BY_NUMBER.get(number)
            member = ({"reference": f"PractitionerRole/{rf.role_id(number)}", "display": rf.display_name(s)}
                      if s else {"display": name})
            existing = next((p for p in ct["participant"] if p["role"][0]["coding"][0]["code"] == role_code
                             and "end" not in p.get("period", {})), None)
            if existing and existing["member"] == member:
                continue
            if existing and since:  # someone else took over the role: the earlier member's period ends
                existing.setdefault("period", {})["end"] = since
            elif existing:
                ct["participant"].remove(existing)
            p = {"role": [{"coding": [{"system": CARE_TEAM_ROLE, "code": role_code, "display": role_display}]}],
                 "member": member, "onBehalfOf": {"reference": f"Organization/{rf.HOSPITAL_ID}"}}
            if since:
                p["period"] = {"start": since}
            ct["participant"].append(p)
        end = e["period"].get("end")
        if discharged and end:
            # the stay is over for the attending and the nurse; a care manager follows the patient up after discharge
            for p in ct["participant"]:
                if p["role"][0]["coding"][0]["code"] != "care-manager":
                    p.setdefault("period", {}).setdefault("end", end)
            if ct["participant"] and all("end" in p.get("period", {}) for p in ct["participant"]):
                ct["status"] = "inactive"
                ct.setdefault("period", {})["end"] = end
        self.care_teams[ct_id] = ct

    # DG1 -> Condition
    def condition(self, dg1, patient_id, encounter_id, visit):
        code, text, system = dg1.get(3, 1).strip(), dg1.get(3, 2), dg1.get(3, 3).upper()
        coding, note, tags = [], None, [SOURCE_TAG]
        cid = fhir_id("rv", visit, "dx", code.replace(".", "") or hashlib.sha1(text.encode()).hexdigest()[:10])
        if cid in self.conditions:
            # e.g. the same admission lists "25000" and "250.00": one diagnosis, sent twice
            self.stats["duplicate_diagnosis_merged"] += 1
        if not code:
            outcome = "no_code_free_text"
        elif system in ("I9", "I9C", "I9CDX"):
            flat = code.replace(".", "")
            if "." in code:
                self.stats["icd9_dotted_normalized"] += 1
            dotted = flat if len(flat) <= 3 or flat.startswith("E") and len(flat) <= 4 else (
                f"{flat[:4]}.{flat[4:]}" if flat.startswith("E") else f"{flat[:3]}.{flat[3:]}")
            coding.append({"system": ICD9CM, "code": dotted, "display": text})
            matches = self.translate(flat) if self.translate else None
            if matches is None:
                outcome = "icd9_not_translated"
            elif not matches:
                outcome = "icd9_no_icd10_match"
            elif len(matches) == 1:
                m = matches[0]
                coding.append({"system": ICD10CM, "code": m["code"], "display": m.get("display", "")})
                outcome = "icd9_upgraded_exact" if m.get("equivalence") in ("equivalent", "equal") else "icd9_upgraded_approximate"
            else:
                options = " or ".join(f"{m['code']} ({m.get('display', '')})" for m in matches)
                note = f"ICD 9 {code} has several ICD 10 options in the CMS mappings: {options}. A coder must choose."
                tags = [SOURCE_TAG, REVIEW_TAG]
                outcome = "icd9_needs_review"
                self.review.append({"visit": visit, "icd9": code, "text": text, "options": [m["code"] for m in matches],
                                    "labels": [m.get("display", "") for m in matches],
                                    "patient": patient_id, "condition": cid})
        elif system in ("I10", "I10C", "ICD10"):
            flat = code.replace(".", "")
            coding.append({"system": ICD10CM, "code": flat if len(flat) <= 3 else f"{flat[:3]}.{flat[3:]}", "display": text})
            outcome = "icd10_at_source"
        else:
            coding.append({"system": f"urn:hl7v2:coding-system:{system or 'unknown'}", "code": code, "display": text})
            outcome = "other_code_system"
        self.dx_outcomes[outcome] += 1

        c = {
            "resourceType": "Condition", "id": cid,
            "meta": {"profile": [f"{US_CORE}/us-core-condition-encounter-diagnosis"], "tag": tags},
            "clinicalStatus": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "active"}]},
            "verificationStatus": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-ver-status", "code": "confirmed"}]},
            "category": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-category", "code": "encounter-diagnosis"}]}],
            "code": {"text": text, **({"coding": coding} if coding else {})},
            "subject": ref("Patient", patient_id),
        }
        if encounter_id:
            c["encounter"] = ref("Encounter", encounter_id)
        if hl7_datetime(dg1.get(5)):
            c["recordedDate"] = hl7_datetime(dg1.get(5))
        if note:
            c["note"] = [{"text": note}]
        self.conditions[cid] = c

    # OBX -> Observation
    def observation(self, obr, obx, patient_id, encounter_id, panel_code):
        code, text, system = obx.get(3, 1), obx.get(3, 2), obx.get(3, 3)
        mapping, how = match_lab(code, text, system, self.synonyms)
        self.lab_matches[how] += 1
        codings = []
        if mapping:
            codings.append({"system": LOINC, "code": mapping["loinc"], "display": mapping["display"]})
        else:
            self.unmapped_labs[text or code] += 1
        if code and system != "LN":
            codings.append({"system": LOCAL_LAB_SYSTEM, "code": code, "display": text})
        vital = panel_code == "VS"
        oid = fhir_id("rv", obr.get(3), obx.get(1))
        o = {
            "resourceType": "Observation", "id": oid,
            "meta": {"profile": [f"{US_CORE}/us-core-{'vital-signs' if vital else 'observation-lab'}"], "tag": [SOURCE_TAG]},
            "status": OBS_STATUS.get(obx.get(11), "final"),
            "category": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                      "code": "vital-signs" if vital else "laboratory"}]}],
            "code": {"text": text or code, **({"coding": codings} if codings else {})},
            "subject": ref("Patient", patient_id),
        }
        if encounter_id:
            o["encounter"] = ref("Encounter", encounter_id)
        when = hl7_datetime(obx.get(14)) or hl7_datetime(obr.get(7))
        if when:
            o["effectiveDateTime"] = when
        lab = obx.get(23, 10) or obx.get(23, 3)  # OBX-23 performing organization: XON-10 identifier (XON-3 before 2.5)
        if lab == rf.LAB_CODE:
            o["performer"] = [{"reference": f"Organization/{rf.LAB_ID}", "display": obx.get(23, 1) or "Riverside Reference Laboratory"}]
            self.stats["results_with_performing_lab"] += 1
        elif obx.get(23, 1):
            o["performer"] = [{"display": obx.get(23, 1)}]

        value, unit, vtype = obx.get(5), obx.get(6), obx.get(2)
        cmp_match = COMPARATOR.match(value)
        if vtype == "NM" and re.fullmatch(r"-?\d+(\.\d+)?", value.strip()):
            o["valueQuantity"] = quantity(number(value.strip()), unit)
            self.stats["value_numeric"] += 1
        elif cmp_match:
            o["valueQuantity"] = {**quantity(number(cmp_match.group(2)), unit), "comparator": cmp_match.group(1)}
            self.stats["value_with_comparator"] += 1
        elif value:
            o["valueString"] = value
            self.stats["value_text"] += 1
        rng = parse_range(obx.get(7))
        if rng:
            o["referenceRange"] = [{"low": quantity(rng[0], unit), "high": quantity(rng[1], unit)}]
        flag = INTERPRETATION.get(obx.get(8).upper())
        if flag:
            o["interpretation"] = [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
                                                "code": flag[0], "display": flag[1]}]}]
        if obx.get(11) == "C":
            self.stats["corrected_results"] += 1
        self.observations[oid] = o

    # one routed message
    def apply(self, msg):
        mtype = msg.message_type
        pid = msg.first("PID")
        patient = self.patient(pid)
        if mtype in ("ADT^A01", "ADT^A04", "ADT^A08") or patient["id"] not in self.patients:
            self.patients[patient["id"]] = patient  # A08 replaces demographics with the newest version
        pv1 = msg.first("PV1")
        evn = msg.first("EVN")
        enc_id = None
        if pv1:
            enc_id = self.encounter(pv1, patient["id"], msg.msh.get(7), discharged=mtype == "ADT^A03",
                                    moved=mtype == "ADT^A02", admitted=mtype in ("ADT^A01", "ADT^A04"),
                                    event_time=hl7_datetime(evn.get(2)) if evn else "")
        if enc_id:
            self.care_team(msg, enc_id, patient["id"], discharged=mtype == "ADT^A03")
        visit = pv1.get(19) if pv1 else ""
        for dg1 in msg.all("DG1"):
            self.condition(dg1, patient["id"], enc_id, visit or patient["id"])
        if mtype == "ORU^R01":
            obr = None
            for seg in msg.segments:
                if seg.name == "OBR":
                    obr = seg
                elif seg.name == "OBX" and obr is not None:
                    self.observation(obr, seg, patient["id"], enc_id, obr.get(4, 1))
        self.stats[f"applied {mtype}"] += 1

    def resources(self):
        """Every resource, ordered so referenced resources come first."""
        return [*self.patients.values(), *self.encounters.values(), *self.care_teams.values(),
                *self.conditions.values(), *self.observations.values()]

    def report(self):
        mapped = sum(v for k, v in self.lab_matches.items() if k != "unmapped")
        total = sum(self.lab_matches.values())
        return {
            "resources": {"Patient": len(self.patients), "Encounter": len(self.encounters), "CareTeam": len(self.care_teams),
                          "Condition": len(self.conditions), "Observation": len(self.observations)},
            "lab_name_matching": dict(self.lab_matches),
            "pct_results_coded_loinc": round(100 * mapped / total, 1) if total else 0.0,
            "unmapped_lab_names": dict(self.unmapped_labs.most_common()),
            "diagnosis_outcomes": dict(self.dx_outcomes),
            "needs_review": self.review,
            "other": dict(self.stats),
        }
