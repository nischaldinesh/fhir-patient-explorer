"""Reference data (master data) of the invented Riverside General Hospital: the FHIR Administration module.

Who and where: the hospital and its lab and payers (Organization), the site, units, rooms and beds (Location),
the staff (Practitioner, PractitionerRole) and the services (HealthcareService). Everything here is invented
demo data and carries DEMO_TAG. Ids are stable, so `pipeline.py reference` loads it with PUT by id and a second
run creates nothing.

The live feed (generate.build_day) assigns beds and staff from these lists, and the mapper (to_fhir.py) turns
PV1-3, PV1-7, ROL and OBX-23 into references to them. Only beds listed here are referenced; any other location
(the original 25 patient feed uses invented room numbers that are not beds here) stays display text only.

  Location tree: hospital site (si) > unit (wa) > room (ro) > bed (bd), linked with Location.partOf.
  Bed ids: riverside-loc-<unit>-<room>-<bed>, for example riverside-loc-med-214-a.
"""
REPO = "https://github.com/nischaldinesh/fhir-patient-explorer"
DEMO_TAG = {"system": f"{REPO}/CodeSystem/source", "code": "riverside-reference",
            "display": "Riverside General reference data (demo, invented)"}
SID = "https://riverside.example.org/fhir/sid"
PHYSICAL_TYPE = "http://terminology.hl7.org/CodeSystem/location-physical-type"
BED_STATUS = "http://terminology.hl7.org/CodeSystem/v2-0116"
ORG_TYPE = "http://terminology.hl7.org/CodeSystem/organization-type"
PRACTITIONER_ROLE = "http://terminology.hl7.org/CodeSystem/practitioner-role"
ROLE_CODE = "http://terminology.hl7.org/CodeSystem/v3-RoleCode"
PHONE = "(555) 555-0100"

HOSPITAL_ID = "riverside-org-hospital"
LAB_ID = "riverside-org-lab"
LAB_CODE = "RRL"  # OBX-23.10, the performing organization identifier the lab sends
SITE_ID = "riverside-loc-hospital"

# (id, name, type code, type display, short code, part of)
ORGANIZATIONS = [
    (HOSPITAL_ID, "Riverside General Hospital", "prov", "Healthcare Provider", "RGH", None),
    (LAB_ID, "Riverside Reference Laboratory", "prov", "Healthcare Provider", LAB_CODE, HOSPITAL_ID),
    ("riverside-org-payer-harborline", "Harborline Health Plan", "pay", "Payer", "HHP", None),
    ("riverside-org-payer-northgate", "Northgate Mutual Health", "pay", "Payer", "NMH", None),
    ("riverside-org-payer-cedar", "Cedar Valley Health Cooperative", "pay", "Payer", "CVH", None),
]

# Units: code (PV1-3.1), name, v3 RoleCode, rooms (PV1-3.2), beds per room (PV1-3.3), room label
UNITS = {
    "ED": ("Emergency Department", "ER", "emergency room", [str(n) for n in range(1, 13)], "A", "Bay"),
    "MED": ("Medical Unit", "HU", "hospital unit", [str(n) for n in range(211, 223)], "AB", "Room"),
    "CARD": ("Cardiac Unit", "HU", "hospital unit", [str(n) for n in range(311, 319)], "AB", "Room"),
    "ICU": ("Intensive Care Unit", "ICU", "intensive care unit", [str(n) for n in range(401, 411)], "A", "Room"),
}
INPATIENT_UNITS = ["MED", "CARD", "ICU"]

# Outpatient clinics: id, name
CLINICS = [
    ("riverside-loc-clinic-cardiology", "Cardiology Clinic"),
    ("riverside-loc-clinic-follow-up", "Discharge Follow Up Clinic"),
    ("riverside-loc-clinic-primary-care", "Primary Care Clinic"),
]

# id, name, type, locations
SERVICES = [
    ("riverside-hcs-emergency", "Emergency Department", "Emergency care", ["riverside-loc-ed"]),
    ("riverside-hcs-inpatient-medicine", "Inpatient Medicine", "Inpatient care",
     ["riverside-loc-med", "riverside-loc-card", "riverside-loc-icu"]),
    ("riverside-hcs-cardiology-clinic", "Cardiology Clinic", "Outpatient cardiology", ["riverside-loc-clinic-cardiology"]),
    ("riverside-hcs-discharge-follow-up", "Discharge Follow Up Clinic", "Transitional care after discharge",
     ["riverside-loc-clinic-follow-up"]),
    ("riverside-hcs-primary-care", "Primary Care", "Primary care", ["riverside-loc-clinic-primary-care"]),
]

# The staff. number: the id the hospital's systems send in PV1-7 and ROL-4 (XCN.1). Invented names; Dana Morgan and
# Jordan Lee are the demo clinician and care manager personas.
# kind: doctor | nurse | care_manager
STAFF = [
    {"number": "1234", "given": "Dana", "family": "Morgan", "kind": "doctor", "title": "Hospitalist",
     "service": "riverside-hcs-inpatient-medicine", "location": "riverside-loc-med"},
    {"number": "2211", "given": "Tomas", "family": "Achebe", "kind": "doctor", "title": "Hospitalist",
     "service": "riverside-hcs-inpatient-medicine", "location": "riverside-loc-med"},
    {"number": "3307", "given": "Rhea", "family": "Lindholm", "kind": "doctor", "title": "Hospitalist",
     "service": "riverside-hcs-inpatient-medicine", "location": "riverside-loc-med"},
    {"number": "4415", "given": "Imani", "family": "Brooks", "kind": "doctor", "title": "Emergency physician",
     "service": "riverside-hcs-emergency", "location": "riverside-loc-ed"},
    {"number": "5120", "given": "Victor", "family": "Halvorsen", "kind": "doctor", "title": "Cardiologist",
     "service": "riverside-hcs-cardiology-clinic", "location": "riverside-loc-card"},
    {"number": "6034", "given": "Leah", "family": "Castellano", "kind": "doctor", "title": "Intensivist",
     "service": "riverside-hcs-inpatient-medicine", "location": "riverside-loc-icu"},
    {"number": "7101", "given": "Marcus", "family": "Webb", "kind": "nurse", "title": "Registered nurse",
     "service": "riverside-hcs-inpatient-medicine", "location": "riverside-loc-med"},
    {"number": "7102", "given": "Ana", "family": "Ruiz", "kind": "nurse", "title": "Registered nurse",
     "service": "riverside-hcs-inpatient-medicine", "location": "riverside-loc-icu"},
    {"number": "7103", "given": "Keiko", "family": "Tanaka", "kind": "nurse", "title": "Registered nurse",
     "service": "riverside-hcs-emergency", "location": "riverside-loc-ed"},
    {"number": "8001", "given": "Jordan", "family": "Lee", "kind": "care_manager", "title": "RN Care Manager",
     "service": "riverside-hcs-discharge-follow-up", "location": "riverside-loc-clinic-follow-up"},
    {"number": "8002", "given": "Tess", "family": "Delgado", "kind": "care_manager", "title": "RN Care Manager",
     "service": "riverside-hcs-discharge-follow-up", "location": "riverside-loc-clinic-follow-up"},
]
STAFF_BY_NUMBER = {s["number"]: s for s in STAFF}


# ---------- ids and HL7 values ----------

def unit_id(unit):
    return f"riverside-loc-{unit.lower()}"


def room_id(unit, room):
    return f"riverside-loc-{unit.lower()}-{room}"


def bed_id(unit, room, bed):
    """The Location id of a bed, or None when the hospital has no such bed."""
    u = UNITS.get((unit or "").upper())
    if not u or room not in u[3] or (bed or "").upper() not in u[4]:
        return None
    return f"riverside-loc-{unit.lower()}-{room}-{bed.lower()}"


def beds(unit):
    """(bed id, PV1-3 value) for every bed of a unit, in a fixed order."""
    _, _, _, rooms, letters, _ = UNITS[unit]
    return [(bed_id(unit, r, b), f"{unit}^{r}^{b}") for r in rooms for b in letters]


ALL_BEDS = {bid: pv1 for unit in UNITS for bid, pv1 in beds(unit)}


def bed_label(unit, room, bed):
    """'Room 214, bed A' or 'Bay 3, bed A'."""
    return f"{UNITS[unit][5]} {room}, bed {bed.upper()}"


def practitioner_id(number):
    return f"riverside-pract-{number}"


def role_id(number):
    return f"riverside-role-{number}"


def display_name(s):
    return f"Dr {s['given']} {s['family']}" if s["kind"] == "doctor" else f"{s['given']} {s['family']}"


def xcn(s):
    """The staff member as an HL7 XCN value: number^family^given, DR prefix for doctors, RN degree for nurses."""
    if s["kind"] == "doctor":
        return f"{s['number']}^{s['family']}^{s['given']}^^^DR"
    return f"{s['number']}^{s['family']}^{s['given']}^^^^RN"


def staff(kind, title=None):
    """Staff numbers of a kind (and title), in roster order."""
    return [s["number"] for s in STAFF if s["kind"] == kind and (title is None or s["title"] == title)]


# ---------- resources ----------

def _meta():
    return {"tag": [DEMO_TAG]}


def _ref(resource_type, rid, display=None):
    r = {"reference": f"{resource_type}/{rid}"}
    if display:
        r["display"] = display
    return r


def organizations():
    out = []
    for rid, name, code, display, short, part_of in ORGANIZATIONS:
        o = {"resourceType": "Organization", "id": rid, "meta": _meta(), "active": True,
             "identifier": [{"system": f"{SID}/organization", "value": short}],
             "type": [{"coding": [{"system": ORG_TYPE, "code": code, "display": display}]}],
             "name": name, "telecom": [{"system": "phone", "value": PHONE}]}
        if part_of:
            o["partOf"] = _ref("Organization", part_of, "Riverside General Hospital")
        out.append(o)
    return out


def _location(rid, name, physical, physical_display, part_of=None, part_of_name=None, type_=None, alias=None):
    loc = {"resourceType": "Location", "id": rid, "meta": _meta(), "status": "active", "mode": "instance",
           "name": name,
           "physicalType": {"coding": [{"system": PHYSICAL_TYPE, "code": physical, "display": physical_display}]},
           "managingOrganization": _ref("Organization", HOSPITAL_ID, "Riverside General Hospital")}
    if alias:
        loc["alias"] = [alias]
    if type_:
        loc["type"] = [type_]
    if part_of:
        loc["partOf"] = _ref("Location", part_of, part_of_name)
    return loc


def bed_location(unit, room, bed, occupied=False):
    """One bed. operationalStatus O (occupied) or U (unoccupied), HL7 v2 table 0116."""
    loc = _location(bed_id(unit, room, bed), bed_label(unit, room, bed), "bd", "Bed",
                    room_id(unit, room), f"{UNITS[unit][5]} {room}", alias=f"{unit} {room} {bed}")
    loc["operationalStatus"] = ({"system": BED_STATUS, "code": "O", "display": "Occupied"} if occupied
                                else {"system": BED_STATUS, "code": "U", "display": "Unoccupied"})
    return loc


def locations(occupied=frozenset()):
    """The site, its units, rooms and beds (beds in `occupied` marked occupied), and the outpatient clinics.
    Parents come before children, so each partOf points at a Location loaded earlier."""
    out = [_location(SITE_ID, "Riverside General Hospital", "si", "Site")]
    for unit, (name, code, code_display, rooms, letters, label) in UNITS.items():
        out.append(_location(unit_id(unit), name, "wa", "Ward", SITE_ID, "Riverside General Hospital",
                             {"coding": [{"system": ROLE_CODE, "code": code, "display": code_display}]}, alias=unit))
    for rid, name in CLINICS:
        out.append(_location(rid, name, "wi", "Wing", SITE_ID, "Riverside General Hospital", {"text": "Outpatient clinic"}))
    for unit, (name, _, _, rooms, letters, label) in UNITS.items():
        for room in rooms:
            out.append(_location(room_id(unit, room), f"{label} {room}", "ro", "Room", unit_id(unit), name))
    for unit, (_, _, _, rooms, letters, _) in UNITS.items():
        for room in rooms:
            for b in letters:
                out.append(bed_location(unit, room, b, bed_id(unit, room, b) in occupied))
    return out


def location_name(rid):
    names = {unit_id(u): v[0] for u, v in UNITS.items()} | dict(CLINICS)
    return names.get(rid, rid)


def healthcare_services():
    return [{"resourceType": "HealthcareService", "id": rid, "meta": _meta(), "active": True, "name": name,
             "providedBy": _ref("Organization", HOSPITAL_ID, "Riverside General Hospital"),
             "type": [{"text": kind}],
             "location": [_ref("Location", loc, location_name(loc)) for loc in locs],
             "telecom": [{"system": "phone", "value": PHONE}]}
            for rid, name, kind, locs in SERVICES]


def practitioners():
    out = []
    for s in STAFF:
        name = {"use": "official", "family": s["family"], "given": [s["given"]]}
        if s["kind"] == "doctor":
            name["prefix"] = ["Dr"]
        out.append({"resourceType": "Practitioner", "id": practitioner_id(s["number"]), "meta": _meta(), "active": True,
                    # clearly fake: an invented staff number at an example.org system, never an NPI
                    "identifier": [{"system": f"{SID}/staff", "value": f"DEMO-{s['number']}"}],
                    "name": [name]})
    return out


SERVICE_NAMES = {rid: name for rid, name, _, _ in SERVICES}


def practitioner_roles():
    out = []
    for s in STAFF:
        code = {"doctor": ("doctor", "Doctor"), "nurse": ("nurse", "Nurse"), "care_manager": ("nurse", "Nurse")}[s["kind"]]
        out.append({"resourceType": "PractitionerRole", "id": role_id(s["number"]), "meta": _meta(), "active": True,
                    "practitioner": _ref("Practitioner", practitioner_id(s["number"]), display_name(s)),
                    "organization": _ref("Organization", HOSPITAL_ID, "Riverside General Hospital"),
                    "code": [{"coding": [{"system": PRACTITIONER_ROLE, "code": code[0], "display": code[1]}],
                              "text": s["title"]}],
                    "location": [_ref("Location", s["location"], location_name(s["location"]))],
                    "healthcareService": [_ref("HealthcareService", s["service"], SERVICE_NAMES[s["service"]])]})
    return out


def resource_groups(occupied=frozenset()):
    """Reference data in load order: every group only points at groups before it."""
    orgs, locs = organizations(), locations(occupied)
    by_type = lambda t: [x for x in locs if x["physicalType"]["coding"][0]["code"] in t]  # noqa: E731
    return [orgs[:1], orgs[1:], by_type({"si"}), by_type({"wa", "wi"}), by_type({"ro"}), by_type({"bd"}),
            healthcare_services(), practitioners(), practitioner_roles()]


def resources(occupied=frozenset()):
    return [r for group in resource_groups(occupied) for r in group]


# A daily run checks these before loading anything; any one missing means the reference data is loaded first.
MARKERS = [("Organization", HOSPITAL_ID), ("Location", "riverside-loc-icu-410-a"),
           ("PractitionerRole", role_id(STAFF[-1]["number"]))]


def bed_updates(before, after):
    """Bed Locations whose occupancy changed between two sets of occupied bed ids."""
    out = []
    for bid in sorted(set(before) ^ set(after)):
        unit, room, b = ALL_BEDS[bid].split("^")
        out.append(bed_location(unit, room, b, bid in after))
    return out
