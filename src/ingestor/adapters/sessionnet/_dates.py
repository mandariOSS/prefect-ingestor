"""Datum-/Zeit-Parsing aus deutschen SessionNet-Strings.

SessionNet-HTML enthält Datums-/Zeit-Angaben in folgenden Formaten:
    - ``"04.05.2026"`` (DD.MM.YYYY) — typisch im title-Attribut
    - ``"16:30 Uhr"`` (HH:MM Uhr) — typisch in der Detail-Liste
    - ``"04.05.2026 16:30 Uhr"`` — kombiniert in manchen Detail-Views

Output: ISO-8601 mit Europe/Berlin-Timezone (was OParl als ``start`` /
``end`` erwartet).
"""

from __future__ import annotations

import re
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

BERLIN = ZoneInfo("Europe/Berlin")

DATE_RE = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b")
TIME_RE = re.compile(r"\b(\d{1,2}):(\d{2})(?:\s*Uhr)?\b")


def parse_date_de(s: str) -> date | None:
    """Findet erstes ``DD.MM.YYYY`` in s und gibt date zurück, sonst None."""
    if not s:
        return None
    m = DATE_RE.search(s)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def parse_time_de(s: str) -> time | None:
    """Findet erstes ``HH:MM`` in s und gibt time zurück, sonst None."""
    if not s:
        return None
    m = TIME_RE.search(s)
    if not m:
        return None
    try:
        return time(int(m.group(1)), int(m.group(2)))
    except ValueError:
        return None


def to_oparl_datetime(d: date, t: time | None) -> str:
    """Kombiniert Datum + Zeit zu OParl-konformem ISO-8601 (Europe/Berlin).

    Wenn keine Zeit vorhanden, wird 00:00 angenommen.
    """
    dt = datetime.combine(d, t or time(0, 0), tzinfo=BERLIN)
    return dt.isoformat()


def split_title_de(title: str) -> tuple[str | None, date | None]:
    """Spaltet ``"<Gremium> <DD.MM.YYYY>"`` in (gremium, date).

    SessionNet ``si0057``-Links haben title-Attribute der Form:
        ``"Details anzeigen: Jugendrat 04.05.2026"``
        ``"Details anzeigen: Ausschuss für Verkehr und Mobilität 06.05.2026"``

    Strategie: regex auf das letzte Datum, alles davor (minus "Details
    anzeigen: "-Prefix) ist der Gremium-Name.
    """
    if not title:
        return None, None
    # "Details anzeigen: " Prefix entfernen
    cleaned = re.sub(r"^Details anzeigen:\s*", "", title).strip()

    # Letztes Datum suchen (für Sicherheit, falls "31.12." mal im Namen steht)
    matches = list(DATE_RE.finditer(cleaned))
    if not matches:
        return cleaned or None, None
    last = matches[-1]
    name = cleaned[: last.start()].strip().rstrip(",")
    try:
        dt = date(int(last.group(3)), int(last.group(2)), int(last.group(1)))
    except ValueError:
        return name or None, None
    return name or None, dt
