"""Geteilte HTTP- und URL-Helfer für alle SessionNet-Sub-Parser."""

from __future__ import annotations

from urllib.parse import urljoin

import httpx
from selectolax.parser import HTMLParser


def resolve(base_url: str, path: str) -> str:
    """Baut absolute URL aus Base-URL + relativem SessionNet-Pfad.

    Behandelt sowohl ``si0040.php`` als auch ``./si0040.php`` und
    ``../sessionnetbi/si0040.php``. Base-URL endet meist auf ``/sessionnetbi/``.
    """
    if not base_url.endswith("/"):
        base_url = base_url + "/"
    return urljoin(base_url, path)


async def fetch_html(client: httpx.AsyncClient, url: str) -> HTMLParser:
    """Lädt eine Seite und parst sie mit selectolax.

    SessionNet liefert manche alten Seiten als ``iso-8859-1`` aus, neuere als
    ``utf-8``. Die ``meta http-equiv="content-type"``-Angabe gewinnt — wir
    lassen httpx das encoding raten, nehmen aber explizit ``response.text``
    (Unicode-decoded) statt ``.content`` (bytes).
    """
    response = await client.get(url)
    response.raise_for_status()
    return HTMLParser(response.text)


def first_text(node, selector: str) -> str | None:
    """Liefert den getrimmten Text des ersten matches oder None."""
    el = node.css_first(selector)
    if el is None:
        return None
    txt = el.text(strip=True)
    return txt or None
