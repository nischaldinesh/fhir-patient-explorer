"""A small HL7 v2 parser, standard library only.

HL7 v2 messages are lines ("segments") separated by carriage returns. Each segment is split into
fields by "|", fields into components by "^", components into subcomponents by "&", and a field
can repeat with "~". The separators are declared in the MSH segment itself, so they are read from
there instead of assumed. Escape sequences such as \\F\\ (a literal "|") are decoded.

Numbering follows the HL7 standard, 1 based: PID-5.1 is field 5, component 1 of PID.
In MSH the field separator itself counts as MSH-1, so MSH-9 is the message type.

Production systems use a library (hl7apy, python-hl7) or an interface engine (Mirth, Rhapsody).
This parser is written out in full to show how the format works.
"""
from dataclasses import dataclass, field


class HL7ParseError(ValueError):
    pass


@dataclass
class Separators:
    field: str = "|"
    component: str = "^"
    repetition: str = "~"
    escape: str = "\\"
    subcomponent: str = "&"


def unescape(text: str, sep: Separators) -> str:
    """Decode \\F\\ \\S\\ \\T\\ \\R\\ \\E\\ and \\.br\\ escape sequences."""
    e = sep.escape
    if e not in text:
        return text
    table = {"F": sep.field, "S": sep.component, "T": sep.subcomponent, "R": sep.repetition, "E": e, ".br": "\n"}
    out, i = [], 0
    while i < len(text):
        if text[i] == e:
            j = text.find(e, i + 1)
            if j > i and text[i + 1:j] in table:
                out.append(table[text[i + 1:j]])
                i = j + 1
                continue
        out.append(text[i])
        i += 1
    return "".join(out)


@dataclass
class Segment:
    name: str
    fields: list  # raw field strings, fields[0] is the segment name
    sep: Separators = field(repr=False, default_factory=Separators)

    def raw(self, n: int) -> str:
        """Raw text of field n, using HL7 numbering."""
        if self.name == "MSH":
            if n == 1:
                return self.sep.field
            n -= 1
        return self.fields[n] if 0 < n < len(self.fields) else ""

    def get(self, n: int, comp: int = 1, rep: int = 1, sub: int = 1) -> str:
        """Decoded value of field n, component comp, repetition rep, subcomponent sub."""
        value = self.raw(n)
        if self.name == "MSH" and n in (1, 2):
            return value
        reps = value.split(self.sep.repetition)
        if rep > len(reps):
            return ""
        comps = reps[rep - 1].split(self.sep.component)
        if comp > len(comps):
            return ""
        subs = comps[comp - 1].split(self.sep.subcomponent)
        return unescape(subs[sub - 1], self.sep) if sub <= len(subs) else ""

    def components(self, n: int, rep: int = 1) -> list:
        value = self.raw(n).split(self.sep.repetition)
        if rep > len(value):
            return []
        return [unescape(c, self.sep) for c in value[rep - 1].split(self.sep.component)]

    def repetitions(self, n: int) -> int:
        value = self.raw(n)
        return len(value.split(self.sep.repetition)) if value else 0


@dataclass
class Message:
    segments: list

    def all(self, name: str) -> list:
        return [s for s in self.segments if s.name == name]

    def first(self, name: str):
        for s in self.segments:
            if s.name == name:
                return s
        return None

    @property
    def msh(self) -> Segment:
        return self.segments[0]

    @property
    def message_type(self) -> str:
        """For example 'ADT^A01' or 'ORU^R01'."""
        code, event = self.msh.get(9, 1), self.msh.get(9, 2)
        return f"{code}^{event}" if event else code

    @property
    def control_id(self) -> str:
        return self.msh.get(10)


def parse(text: str) -> Message:
    """Parse one HL7 v2 message. Accepts \\r, \\n or \\r\\n between segments."""
    text = text.lstrip("﻿\x0b").rstrip("\x1c\r\n ")  # BOM and MLLP framing characters
    lines = [ln for ln in text.replace("\r\n", "\r").replace("\n", "\r").split("\r") if ln.strip()]
    if not lines:
        raise HL7ParseError("empty message")
    if not lines[0].startswith("MSH") or len(lines[0]) < 8:
        raise HL7ParseError("message does not start with an MSH segment")
    fs = lines[0][3]
    enc = lines[0][4:8]
    if len(set(enc + fs)) != 5:
        raise HL7ParseError(f"invalid encoding characters {enc!r}")
    sep = Separators(field=fs, component=enc[0], repetition=enc[1], escape=enc[2], subcomponent=enc[3])
    segments = []
    for ln in lines:
        name = ln[:3]
        if len(name) != 3 or not name.isalnum() or (len(ln) > 3 and ln[3] != fs):
            raise HL7ParseError(f"malformed segment: {ln[:20]!r}")
        segments.append(Segment(name, ln.split(fs), sep))
    msg = Message(segments)
    if not msg.msh.get(9, 1):
        raise HL7ParseError("MSH-9 message type is empty")
    return msg


# Names of the fields this project reads, for the `show` command and for learning the format.
FIELD_NAMES = {
    "MSH": {3: "Sending application", 4: "Sending facility", 7: "Message date/time", 9: "Message type",
            10: "Message control ID", 11: "Processing ID", 12: "Version"},
    "EVN": {1: "Event type", 2: "Recorded date/time"},
    "PID": {3: "Patient identifier (MRN)", 5: "Patient name", 7: "Date of birth", 8: "Administrative sex",
            10: "Race", 11: "Address", 13: "Home phone"},
    "PV1": {2: "Patient class", 3: "Assigned location", 4: "Admission type", 6: "Prior location",
            7: "Attending doctor", 19: "Visit number", 44: "Admit date/time", 45: "Discharge date/time"},
    "ROL": {2: "Action code", 3: "Role", 4: "Role person", 5: "Role begin date/time"},
    "DG1": {1: "Set ID", 3: "Diagnosis code", 5: "Diagnosis date/time", 6: "Diagnosis type"},
    "OBR": {1: "Set ID", 3: "Filler order number", 4: "Universal service ID", 7: "Observation date/time"},
    "OBX": {1: "Set ID", 2: "Value type", 3: "Observation identifier", 5: "Observation value", 6: "Units",
            7: "Reference range", 8: "Abnormal flag", 11: "Result status", 14: "Observation date/time",
            23: "Performing organization"},
    "MSA": {1: "Acknowledgment code", 2: "Message control ID"},
    "QRD": {1: "Query date/time", 8: "Who subject filter"},
}


def describe(msg: Message) -> str:
    """Human readable dump of the fields this project uses."""
    lines = [f"{msg.message_type}  control id {msg.control_id}"]
    for seg in msg.segments:
        names = FIELD_NAMES.get(seg.name, {})
        lines.append(seg.name)
        for n, label in names.items():
            value = seg.raw(n)
            if value:
                lines.append(f"  {seg.name}-{n:<3} {label:<26} {value}")
    return "\n".join(lines)
