"""SessionNet meeting parser.

Liest die Sitzungsliste (``si0040.php``) und yieldet OParl-Meeting-Dicts.
Optional reichert pro Meeting via Detail-Page (``si0057.php?__ksinr=N``)
an, falls ``include_details=True`` im Source-Config gesetzt ist (langsamer,
aber liefert TOPs/Vorlagen-Cross-References).

HTML-Struktur (modernes SessionNet V:050500+):

    <a href="si0057.php?__ksinr=14414"
       title="Details anzeigen: Jugendrat 04.05.2026"
       class="smce-a-u smc-link-normal smc_doc smc_datatype_si">
       Jugendrat
    </a>

    Begleitende ``<ul class="list-inline smc-detail-list">``:
        <li class="list-inline-item">16:30 Uhr</li>
        <li class="list-inline-item">Raum 2/1, 11. Etage, Stadthaus 2, ...</li>

    Dokument-Anhänge (Einladung, Tagesordnung, Niederschrift):
        <a href="getfile.php?id=584088&type=do" ...>Einladung öffentlich</a>
"""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncIterator
from datetime import datetime
from urllib.parse import urljoin

import httpx
from selectolax.parser import HTMLParser, Node

from ingestor.adapters.sessionnet._dates import (
    parse_time_de,
    split_title_de,
    to_oparl_datetime,
)
from ingestor.adapters.sessionnet._html import fetch_html, resolve

logger = logging.getLogger(__name__)


# SessionNet pagination: normalerweise sind alle aktuellen Sitzungen auf einer
# Seite (Standard: nächste 30 Tage). Wir können YY/MM-Filter anhängen, um
# historische Daten zu holen.
MEETING_LIST_PATH = "si0040.php"


def _resolve_meeting_url(base_url: str, ksinr: int) -> str:
    return resolve(base_url, f"si0057.php?__ksinr={ksinr}")


def _parse_meeting_row(base_url: str, anchor: Node) -> dict | None:
    """Aus einem ``<a href="si0057.php?__ksinr=N" title="...">`` ein OParl-Meeting bauen.

    Returns None wenn die Row nicht parst (z.B. fehlendes Datum im title).
    """
    href = anchor.attributes.get("href", "")
    m = re.search(r"__ksinr=(\d+)", href)
    if not m:
        return None
    ksinr = int(m.group(1))

    title = anchor.attributes.get("title", "")
    name, meeting_date = split_title_de(title)
    if name is None or meeting_date is None:
        # Wir akzeptieren auch Meetings ohne klares Datum, aber ohne IRGENDEIN
        # Identifier ist ein Meeting unbrauchbar.
        logger.debug("Skip meeting %s — title parse failed: %r", ksinr, title)
        return None

    meeting_url = _resolve_meeting_url(base_url, ksinr)

    # Zeit aus dem nachfolgenden detail-list <ul> auslesen, wenn vorhanden.
    # In der Tabelle ist dieser <ul> der nächste Sibling des umschließenden
    # <div class="smc-el-h"> innerhalb derselben <td class="silink">.
    parent_td = _ascend_to(anchor, lambda n: "silink" in n.attributes.get("class", ""))
    start_time = None
    location_text = None
    if parent_td is not None:
        details = parent_td.css_first("ul.smc-detail-list")
        if details is not None:
            items = [li.text(strip=True) for li in details.css("li.list-inline-item")]
            for item in items:
                if start_time is None:
                    t = parse_time_de(item)
                    if t is not None:
                        start_time = t
                        continue
                # Erstes Nicht-Zeit-Item → Location
                if location_text is None and item:
                    location_text = item

    start_iso = to_oparl_datetime(meeting_date, start_time)

    meeting: dict = {
        "id": meeting_url,
        "type": "https://schema.oparl.org/1.1/Meeting",
        "name": f"{name} {meeting_date.strftime('%d.%m.%Y')}",
        "start": start_iso,
        # SessionNet liefert kein End-Datum auf der Liste — bleibt für Detail-View
        # Mandari-Vendor-Extensions:
        "mandari:adapter": "ris_sessionnet",
        "mandari:ksinr": ksinr,
    }
    if location_text:
        # Inline-Location als String — eine vollständige Location-Entity
        # erfordert getrennten Detail-Abruf. Wir hängen sie als Vendor-Field an.
        meeting["mandari:locationText"] = location_text

    # Anhänge (Einladung, Tagesordnung, Niederschrift) als File-Refs
    if parent_td is not None:
        # Die Documents-Cell ist der nächste <td>-Sibling des silink-<td>
        docs_td = parent_td.parent
        if docs_td is not None:
            docs_cell = docs_td.css_first("td.sidocs")
            if docs_cell is not None:
                files: list[dict] = []
                for a in docs_cell.css("a[href*='getfile.php']"):
                    fhref = a.attributes.get("href", "")
                    file_url = urljoin(base_url + "/", fhref)
                    file_label = a.attributes.get("title") or a.text(strip=True)
                    files.append({
                        "id": file_url,
                        "type": "https://schema.oparl.org/1.1/File",
                        "name": file_label,
                        "accessUrl": file_url,
                        "fileName": _extract_filename(file_label),
                        "mimeType": "application/pdf",  # default — getfile liefert idR PDFs
                        "mandari:adapter": "ris_sessionnet",
                    })
                if files:
                    meeting["auxiliaryFile"] = files

    return meeting


def _ascend_to(node: Node, predicate) -> Node | None:
    """Klettert die Parent-Kette hoch bis zum ersten Match oder None."""
    cur = node.parent
    while cur is not None:
        if predicate(cur):
            return cur
        cur = cur.parent
    return None


def _extract_filename(label: str) -> str:
    """Macht aus 'Einladung öffentlich' einen Dateinamen-Stub."""
    safe = re.sub(r"[^\w\-äöüÄÖÜß]+", "_", label or "datei")
    return safe.strip("_") + ".pdf"


async def list_meetings(
    client: httpx.AsyncClient,
    base_url: str,
    *,
    modified_since: datetime | None = None,  # noqa: ARG001 — SessionNet hat kein incremental
    max_pages: int = 5,
) -> AsyncIterator[dict]:
    """Iteriert die Sitzungsliste und yieldet OParl-Meeting-Dicts.

    Pagination: SessionNet zeigt per Default die nächsten ~30 Tage. Für
    historische Daten könnte man ``YY=2026&MM=04`` anhängen — aktuell
    holen wir nur die Default-View (das ist „aktuelle und kommende
    Sitzungen", was 99 % der Sync-Use-Cases abdeckt).

    ``max_pages`` ist eine Sicherung gegen Endlos-Loops, falls SessionNet
    irgendwann doch eine Pagination einbaut.
    """
    list_url = resolve(base_url, MEETING_LIST_PATH)
    seen: set[int] = set()
    pages = 0

    while pages < max_pages:
        pages += 1
        try:
            tree = await fetch_html(client, list_url)
        except httpx.HTTPError as exc:
            logger.warning("SessionNet meeting-list fetch failed: %s", exc)
            return

        anchors = tree.css("a[href*='si0057.php']")
        new_count = 0
        for anchor in anchors:
            meeting = _parse_meeting_row(base_url, anchor)
            if meeting is None:
                continue
            ksinr = meeting.get("mandari:ksinr")
            if ksinr in seen:
                continue
            seen.add(ksinr)
            new_count += 1
            yield meeting

        # Nächste Seite suchen — typisch ein <a class="next">. Default-View
        # hat keinen, also brechen wir hier idR sofort ab.
        next_link = tree.css_first("a.next, a[rel='next']")
        if next_link is None or new_count == 0:
            return
        href = next_link.attributes.get("href")
        if not href:
            return
        list_url = urljoin(list_url, href)
