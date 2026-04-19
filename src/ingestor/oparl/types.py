"""
OParl-Typ-Erkennung (1.0 + 1.1 kompatibel).

Jedes OParl-Objekt hat ein `type`-Feld mit einer URL, die den Typ identifiziert.
Die Spezifikation erlaubt Variationen ("oparl/1.0/Body" vs "oparl/1.1/Body").
"""

from enum import StrEnum


class OParlType(StrEnum):
    """OParl-Entity-Typen."""

    SYSTEM = "System"
    BODY = "Body"
    ORGANIZATION = "Organization"
    PERSON = "Person"
    MEMBERSHIP = "Membership"
    MEETING = "Meeting"
    AGENDA_ITEM = "AgendaItem"
    PAPER = "Paper"
    CONSULTATION = "Consultation"
    FILE = "File"
    LOCATION = "Location"
    LEGISLATIVE_TERM = "LegislativeTerm"

    @classmethod
    def from_url(cls, url: str) -> "OParlType | None":
        """Erkennt OParl-Typ aus type-URL (z.B. 'https://schema.oparl.org/1.1/Body')."""
        if not url:
            return None
        # Nach letztem / nehmen
        type_name = url.rstrip("/").rsplit("/", 1)[-1]
        try:
            return cls(type_name)
        except ValueError:
            return None


def detect_oparl_type(data: dict) -> OParlType | None:
    """Erkennt den OParl-Typ aus einem JSON-Objekt."""
    if not isinstance(data, dict):
        return None
    return OParlType.from_url(data.get("type", ""))
