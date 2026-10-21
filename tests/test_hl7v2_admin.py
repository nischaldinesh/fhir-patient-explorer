"""The hospital's "who and where": reference data, beds, transfers, attending, care teams, performing lab."""
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "hl7v2"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import feed_state  # noqa: E402
import generate  # noqa: E402
import pipeline  # noqa: E402
import reference as rf  # noqa: E402
import to_fhir as tf  # noqa: E402
from hl7 import parse  # noqa: E402
from route import Router  # noqa: E402
from test_hl7v2_daily import FakeFhir, daily_args  # noqa: E402

DAY = date(2026, 9, 1)


def simulate(days, start=DAY, state=None):
    state, out = state or feed_state.FeedState(), []
    for k in range(days):
        out.append(generate.build_day(start + timedelta(days=k), state))
    return state, out


def mapped(messages, seed=()):
    router, mapper = Router(), tf.FeedMapper(translate=None)
    mapper.seed(seed)
    for _, _, text in messages:
        o = router.route(text)
        if o.status == "process":
            mapper.apply(o.message)
    return mapper


@pytest.fixture(scope="module")
def four_weeks():
    return simulate(28)


@pytest.fixture
def fhir():
    f = FakeFhir()
    yield f
    f.srv.shutdown()


# ---------- reference data ----------

def test_reference_counts_and_ids():
    res = rf.resources()
    kinds = {}
    for r in res:
        kinds[r["resourceType"]] = kinds.get(r["resourceType"], 0) + 1
    assert kinds == {"Organization": 5, "Location": 112, "HealthcareService": 5, "Practitioner": 11, "PractitionerRole": 11}
    ids = [(r["resourceType"], r["id"]) for r in res]
    assert len(ids) == len(set(ids))
    assert all(r["id"] == tf.fhir_id(r["id"]) for r in res)
    assert all(rf.DEMO_TAG in r["meta"]["tag"] for r in res)  # every piece of reference data is marked demo
    beds = [r for r in res if r["resourceType"] == "Location" and r["physicalType"]["coding"][0]["code"] == "bd"]
    assert len(beds) == 12 + 24 + 16 + 10 and len(rf.ALL_BEDS) == len(beds)
    assert "riverside-loc-med-214-a" in rf.ALL_BEDS and rf.ALL_BEDS["riverside-loc-med-214-a"] == "MED^214^A"
    assert all(b["operationalStatus"]["code"] == "U" for b in beds)
    names = {r["name"] for r in res if r["resourceType"] == "HealthcareService"}
    assert names == {"Emergency Department", "Inpatient Medicine", "Cardiology Clinic", "Discharge Follow Up Clinic", "Primary Care"}


def test_reference_points_only_at_reference_data_loaded_before_it():
    loaded = set()
    for group in rf.resource_groups():
        for r in group:
            refs = re.findall(r"'reference': '(\w+/[a-z0-9-]+)'", str(r))
            assert refs or r["resourceType"] in ("Practitioner", "Organization"), r["id"]
            for x in refs:
                assert x in loaded, f"{r['resourceType']}/{r['id']} points at {x} before it is loaded"
        loaded |= {f"{r['resourceType']}/{r['id']}" for r in group}


def test_location_tree_and_bed_status():
    locs = {r["id"]: r for r in rf.locations(occupied={"riverside-loc-icu-401-a"})}
    bed = locs["riverside-loc-med-214-a"]
    assert bed["name"] == "Room 214, bed A" and bed["partOf"]["reference"] == "Location/riverside-loc-med-214"
    assert locs["riverside-loc-med-214"]["partOf"]["reference"] == "Location/riverside-loc-med"
    assert locs["riverside-loc-med"]["partOf"]["reference"] == f"Location/{rf.SITE_ID}"
    assert locs[rf.SITE_ID]["physicalType"]["coding"][0]["code"] == "si"
    assert locs["riverside-loc-icu-401-a"]["operationalStatus"]["code"] == "O"
    assert rf.bed_id("ED", "3", "a") == "riverside-loc-ed-3-a"
    assert rf.bed_id("MED", "214", "C") is None and rf.bed_id("XYZ", "1", "A") is None and rf.bed_id("", "", "") is None
    changed = rf.bed_updates({"riverside-loc-med-214-a", "riverside-loc-icu-401-a"}, {"riverside-loc-icu-401-a", "riverside-loc-ed-1-a"})
    assert [(b["id"], b["operationalStatus"]["code"]) for b in changed] == [("riverside-loc-ed-1-a", "O"), ("riverside-loc-med-214-a", "U")]


def test_demo_personas_are_on_the_staff_list():
    pract = {r["id"]: r for r in rf.practitioners()}
    assert pract["riverside-pract-1234"]["name"][0]["family"] == "Morgan"
    assert pract["riverside-pract-8001"]["name"][0]["given"] == ["Jordan"]
    assert all(p["identifier"][0]["value"].startswith("DEMO-") for p in pract.values())  # clearly fake identifiers


def test_reference_load_is_idempotent(tmp_path, fhir, capsys):
    args = ["reference", "--base", fhir.base, "--state", str(tmp_path / "state.json")]
    assert pipeline.main(args) == 0
    assert f"({len(rf.resources())} new)" in capsys.readouterr().out
    first = dict(fhir.store)
    assert pipeline.main(args) == 0
    assert "(0 new)" in capsys.readouterr().out and fhir.store == first
    assert pipeline.reference_missing(fhir.base) is False


# ---------- the generator ----------

def stays_by_bed(days):
    """(start, end) per bed from the ADT events, every message counted once (resends excluded)."""
    seen, current, stays = set(), {}, {}

    def close(visit, when):
        if visit in current:
            bed, since = current.pop(visit)
            stays.setdefault(bed, []).append((since, when))

    for msgs in days:
        for _, label, text in msgs:
            if text in seen or label not in ("ADT_A01", "ADT_A02", "ADT_A03", "ADT_A04"):
                continue
            seen.add(text)
            m = parse(text)
            v = m.first("PV1")
            when = m.first("EVN").get(2)
            if label == "ADT_A03":
                close(v.get(19), v.get(45))
                continue
            close(v.get(19), when)
            current[v.get(19)] = (rf.bed_id(v.get(3, 1), v.get(3, 2), v.get(3, 3)), when)
    for visit, (bed, since) in current.items():
        stays.setdefault(bed, []).append((since, "99999999999999"))
    return stays


def test_no_bed_ever_holds_two_patients(four_weeks):
    _, days = four_weeks
    stays = stays_by_bed(days)
    assert None not in stays  # every ADT names a real bed
    assert len(stays) > 30
    for bed, intervals in stays.items():
        intervals.sort()
        for (s1, e1), (s2, e2) in zip(intervals, intervals[1:]):
            assert e1 <= s2, f"{bed}: a stay until {e1} overlaps one from {s2}"


def test_transfers_happen_and_name_the_prior_location(four_weeks):
    _, days = four_weeks
    a02 = [parse(t) for msgs in days for _, label, t in msgs if label == "ADT_A02"]
    moves = {(m.first("PV1").get(6, 1), m.first("PV1").get(3, 1)) for m in a02}
    assert ("ED", "MED") in moves or ("ED", "CARD") in moves  # up from the emergency department
    assert any(to == "ICU" and frm != "ED" for frm, to in moves)  # to the ICU from a unit
    assert any(frm == "ICU" and to in ("MED", "CARD") for frm, to in moves)  # and back
    assert all(m.first("PV1").get(6) and m.first("PV1").get(6) != m.first("PV1").get(3) for m in a02)
    totals = [generate.day_summary(msgs)["transfers"] for msgs in days]
    assert sum(totals) == len({t for msgs in days for _, label, t in msgs if label == "ADT_A02"})


def test_attending_nurse_care_manager_and_lab_in_the_messages(four_weeks):
    _, days = four_weeks
    msgs = [t for d in days for _, _, t in d]
    staff = {s["number"] for s in rf.STAFF}
    for t in msgs:
        if "|ADT^A0" in t:
            assert parse(t).first("PV1").get(7, 1) in staff
    discharges = [parse(t) for d in days for _, label, t in d if label == "ADT_A03" and "\rPV1|1|I|" in t]
    assert discharges and all(any(r.get(3, 1) == "CM" for r in m.all("ROL")) for m in discharges)
    labs = [parse(t) for d in days for _, label, t in d if label == "ORU_R01" and "|LABSYS|" in t]
    assert all(o.get(23, 10) == rf.LAB_CODE for m in labs for o in m.all("OBX"))


def test_original_feed_has_no_bed_of_the_hospital():
    """The original 25 patient feed stays byte identical, and none of its invented rooms is a bed here, so its
    stays never show a current bed or take one from the live feed."""
    for _, _, t in generate.build():
        if t.startswith("MSH") and "\rPV1|" in t:
            v = parse(t).first("PV1")
            assert rf.bed_id(v.get(3, 1), v.get(3, 2), v.get(3, 3)) is None


def test_a_stay_from_before_beds_were_tracked_moves_to_a_real_bed():
    state, _ = simulate(2)
    adm = state.open_admissions[0]
    for k in ("bed", "bed_since", "roles", "moves"):
        adm.pop(k, None)
    adm["unit"] = "MED^450^B"  # an old random room number
    msgs = generate.build_day(DAY + timedelta(days=2), state)
    a02 = [parse(t) for _, label, t in msgs if label == "ADT_A02" and adm["visit"] in t]
    assert a02 and a02[0].first("PV1").raw(6) == "MED^450^B"
    assert rf.bed_id(*a02[0].first("PV1").raw(3).split("^")) is not None


# ---------- the mapper ----------

MSH = "MSH|^~\\&|EPICADT|RIVERSIDE^RIVERSIDE GENERAL HOSPITAL|FHIRBRIDGE|FHIRBRIDGE|{ts}||{type}|{cid}|P|2.5.1"
PID = "PID|1||RV200001^^^RIVERSIDE^MR||Okafor^Maya||19700101|F"


def adt(mtype, ts, location, prior="", discharge="", extra=()):
    fields = [""] * 46
    for k, v in {1: "1", 2: "I", 3: location, 6: prior, 7: "1234^Morgan^Dana^^^DR", 19: "V2026090101",
                 44: "20260901080000", 45: discharge}.items():
        fields[k] = v
    return parse("\r".join([MSH.format(ts=ts, type=mtype, cid=f"C{ts}"), f"EVN|{mtype[-3:]}|{ts}", PID,
                            "PV1|" + "|".join(fields[1:]), *extra]))


NURSE = "ROL|V2026090101PN|AD|PN^Primary nurse^L|7101^Webb^Marcus^^^^RN|20260901080000"


def test_a_transfer_completes_the_old_location_and_adds_the_new_one():
    m = tf.FeedMapper()
    m.apply(adt("ADT^A01", "20260901080000", "ED^3^A", extra=[NURSE]))
    m.apply(adt("ADT^A02", "20260901103000", "MED^214^A", prior="ED^3^A", extra=[NURSE]))
    m.apply(adt("ADT^A08", "20260901120000", "ED^3^A"))  # a late message with the old place moves nobody
    e = m.encounters["rv-v2026090101"]
    locs = [(x["location"]["reference"], x["status"], x["period"].get("start"), x["period"].get("end")) for x in e["location"]]
    assert locs == [
        ("Location/riverside-loc-ed", "completed", "2026-09-01T08:00:00", "2026-09-01T10:30:00"),
        ("Location/riverside-loc-ed-3-a", "completed", "2026-09-01T08:00:00", "2026-09-01T10:30:00"),
        ("Location/riverside-loc-med", "active", "2026-09-01T10:30:00", None),
        ("Location/riverside-loc-med-214-a", "active", "2026-09-01T10:30:00", None),
    ]
    assert e["location"][3]["location"]["display"] == "Room 214, bed A"
    assert m.stats["location_change_without_transfer_ignored"] == 1 and m.stats["transfers_applied"] == 1

    # same transfer again (a rerun) changes nothing; the discharge completes the bed
    m.apply(adt("ADT^A02", "20260901103000", "MED^214^A", prior="ED^3^A"))
    assert len(m.encounters["rv-v2026090101"]["location"]) == 4
    m.apply(adt("ADT^A03", "20260904100000", "MED^214^A", discharge="20260904100000",
                extra=[NURSE, "ROL|V2026090101CM|AD|CM^Care manager^L|8001^Lee^Jordan^^^^RN|20260904100000"]))
    e = m.encounters["rv-v2026090101"]
    assert all(x["status"] == "completed" for x in e["location"]) and e["location"][3]["period"]["end"] == "2026-09-04T10:00:00"


def test_attending_and_care_team():
    m = tf.FeedMapper()
    m.apply(adt("ADT^A01", "20260901080000", "MED^214^A", extra=[NURSE]))
    e = m.encounters["rv-v2026090101"]
    assert e["participant"] == [{"type": [tf.ATTENDER],
                                 "individual": {"reference": "Practitioner/riverside-pract-1234", "display": "Dr Dana Morgan"}}]
    ct = m.care_teams["rv-v2026090101-careteam"]
    assert ct["encounter"] == {"reference": "Encounter/rv-v2026090101"} and ct["status"] == "active"
    assert [(p["role"][0]["coding"][0]["code"], p["member"]["reference"]) for p in ct["participant"]] == [
        ("attending", "PractitionerRole/riverside-role-1234"), ("primary-nurse", "PractitionerRole/riverside-role-7101")]
    m.apply(adt("ADT^A03", "20260904100000", "MED^214^A", discharge="20260904100000",
                extra=[NURSE, "ROL|V2026090101CM|AD|CM^Care manager^L|8001^Lee^Jordan^^^^RN|20260904100000"]))
    ct = m.care_teams["rv-v2026090101-careteam"]
    cm = ct["participant"][2]
    assert cm["member"]["reference"] == "PractitionerRole/riverside-role-8001" and cm["period"] == {"start": "2026-09-04T10:00:00"}
    assert ct["participant"][0]["period"]["end"] == "2026-09-04T10:00:00"
    assert ct["status"] == "active"  # the care manager follows the patient up after discharge


def test_unknown_attending_and_place_stay_text():
    m = tf.FeedMapper()
    msg = adt("ADT^A01", "20260901080000", "ICU^301^A")
    msg.first("PV1").fields[7] = "9999^Nobody^Al^^^DR"
    m.apply(msg)
    e = m.encounters["rv-v2026090101"]
    assert e["location"] == [{"location": {"display": "ICU 301 A"}}]
    assert e["participant"][0]["individual"] == {"display": "Dr Al Nobody"} and m.stats["attending_not_on_staff_list"] == 1


def test_observation_performer_is_the_lab():
    m = tf.FeedMapper()
    m.apply(parse("\r".join([MSH.format(ts="20260901120000", type="ORU^R01", cid="R1"), PID, "PV1|1|I|||||||||||||||||V2026090101",
                             "OBR|1||L1|BMP^Basic metabolic panel^L|||20260901110000",
                             "OBX|1|NM|GLU^Glucose^L||99|mg/dL|70-105|N|||F|||20260901110000|||||||||" + generate.LAB_XON,
                             "OBX|2|NM|NA^Sodium^L||140|mmol/L|133-145|N|||F|||20260901110000"])))
    assert m.observations["rv-l1-1"]["performer"] == [{"reference": f"Organization/{rf.LAB_ID}", "display": "Riverside Reference Laboratory"}]
    assert "performer" not in m.observations["rv-l1-2"]


def test_every_daily_encounter_has_an_attending_and_a_care_team(four_weeks):
    _, days = four_weeks
    lab_results = 0
    for msgs in days:
        m = mapped(msgs)
        for eid, e in m.encounters.items():
            att = e["participant"][0]
            assert att["type"] == [tf.ATTENDER] and att["individual"]["reference"].startswith("Practitioner/riverside-pract-")
            assert f"{eid}-careteam" in m.care_teams
        for o in m.observations.values():
            if o["category"][0]["coding"][0]["code"] == "laboratory":
                assert o["performer"][0]["reference"] == f"Organization/{rf.LAB_ID}"
                lab_results += 1
    assert lab_results > 100


def test_seeding_keeps_the_location_history_across_days(four_weeks):
    """A day's mapper starts from what the server holds for open visits, so yesterday's moves are kept."""
    _, days = four_weeks
    server = {}
    for msgs in days:
        m = mapped(msgs, seed=[r for r in server.values() if r["resourceType"] in ("Encounter", "CareTeam")])
        server.update({(r["resourceType"], r["id"]): r for r in m.resources()})
    encounters = [r for (t, _), r in server.items() if t == "Encounter"]
    moved = [e for e in encounters if len([x for x in e["location"] if x["physicalType"]["coding"][0]["code"] == "bd"]) >= 3]
    assert moved  # admitted, moved to the ICU on a later day, and back
    for e in encounters:
        beds = [x for x in e["location"] if x["physicalType"]["coding"][0]["code"] == "bd"]
        assert sum(1 for x in beds if x["status"] == "active") == (1 if e["status"] == "in-progress" else 0)
        for a, b in zip(beds, beds[1:]):
            assert a["status"] == "completed" and a["period"]["end"] == b["period"]["start"]


# ---------- the daily run against a fake server ----------

def test_backfill_loads_reference_first_then_beds_and_care_teams(tmp_path, fhir, monkeypatch):
    monkeypatch.setattr(pipeline, "today_utc", lambda: DAY + timedelta(days=4))
    assert pipeline.main(daily_args(tmp_path, fhir.base, "backfill", "--days", "4")) == 0
    store = fhir.store
    for r in rf.resources():
        assert f"{r['resourceType']}/{r['id']}" in store
    encounters = [r for k, r in store.items() if k.startswith("Encounter/")]
    assert encounters and all(f"CareTeam/{e['id']}-careteam" in store for e in encounters)
    for e in encounters:
        for x in e.get("location", []):
            assert x["location"]["reference"] in {f"Location/{i}" for i in [*rf.ALL_BEDS, *map(rf.unit_id, rf.UNITS)]}
    state = feed_state.load(tmp_path / "state.json")
    occupied = {k.split("/")[1] for k, r in store.items()
                if k.startswith("Location/") and r.get("operationalStatus", {}).get("code") == "O"}
    assert occupied == {a["bed"] for a in state.open_admissions}
    assert sum(r["transfers"] for r in fhir.history()) >= 1
