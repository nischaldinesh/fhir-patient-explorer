"""Route HL7 v2 messages the way an interface engine does: decide what each message is before
touching it, process only what we handle, and keep a reason for everything else.

Outcomes:
  process    a clinical message type we handle (admissions, transfers, discharges, updates, lab results)
  skip       valid HL7, but no clinical data (ACK, QRY) or a type we do not handle yet (MDM, SIU)
  duplicate  same message control ID (MSH-10) already received: a resend, ignored
  error      cannot be parsed, or is missing something we need; quarantined for a person to look at
"""
from dataclasses import dataclass, field

from hl7 import HL7ParseError, parse

HANDLED = {
    "ADT^A01": "Admit inpatient",
    "ADT^A02": "Transfer to another bed",
    "ADT^A03": "Discharge",
    "ADT^A04": "Register outpatient",
    "ADT^A08": "Update patient information",
    "ORU^R01": "Observation results",
}
SKIP_REASONS = {
    "ACK": "Acknowledgment: confirms receipt, carries no clinical data",
    "QRY": "Query: asks another system for data, carries no clinical data",
    "MDM": "Medical document message: not handled by this pipeline yet",
    "SIU": "Scheduling message: not handled by this pipeline yet",
}


@dataclass
class Outcome:
    status: str  # process | skip | duplicate | error
    message_type: str = ""
    control_id: str = ""
    reason: str = ""
    message: object = field(default=None, repr=False)


def validate(msg) -> str:
    """Return a problem description, or "" when the message has what the mapper needs."""
    pid = msg.first("PID")
    if pid is None:
        return "no PID segment, cannot tell which patient this is about"
    if not pid.get(3, 1):
        return "PID-3 patient identifier is empty"
    if msg.message_type == "ORU^R01" and not msg.all("OBX"):
        return "ORU message without any OBX result"
    return ""


class Router:
    def __init__(self, seen=None):
        """seen: control IDs already received in earlier runs, so a resend is caught across days."""
        self.seen = set(seen or ())

    def route(self, text: str) -> Outcome:
        try:
            msg = parse(text)
        except HL7ParseError as e:
            return Outcome("error", reason=f"cannot parse: {e}")
        mtype, cid = msg.message_type, msg.control_id
        if mtype in HANDLED:
            if cid and cid in self.seen:
                return Outcome("duplicate", mtype, cid, f"control ID {cid} already received", msg)
            problem = validate(msg)
            if problem:
                return Outcome("error", mtype, cid, problem, msg)
            if cid:
                self.seen.add(cid)
            return Outcome("process", mtype, cid, HANDLED[mtype], msg)
        code = mtype.split("^")[0]
        reason = SKIP_REASONS.get(code) or SKIP_REASONS.get(mtype) or f"message type {mtype} is not handled"
        return Outcome("skip", mtype, cid, reason, msg)
