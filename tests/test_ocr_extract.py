"""
Tests für OCR-Extraktions-Helfer (ohne DB, ohne externe APIs).
"""

from __future__ import annotations

from ingestor.flows.extract_text import _extract_pypdf


MINIMAL_PDF_WITH_TEXT = (
    b"%PDF-1.4\n"
    b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 300 144]/Contents 4 0 R/Resources<</Font<</F1 5 0 R>>>>>>endobj\n"
    b"4 0 obj<</Length 55>>stream\n"
    b"BT /F1 12 Tf 50 100 Td (Hello OParl Ingestor Test) Tj ET\n"
    b"endstream\nendobj\n"
    b"5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n"
    b"xref\n0 6\n"
    b"0000000000 65535 f \n"
    b"0000000009 00000 n \n"
    b"0000000055 00000 n \n"
    b"0000000101 00000 n \n"
    b"0000000195 00000 n \n"
    b"0000000295 00000 n \n"
    b"trailer<</Size 6/Root 1 0 R>>\nstartxref\n355\n%%EOF"
)


def test_pypdf_returns_empty_on_garbage_bytes() -> None:
    text, pages = _extract_pypdf(b"not a pdf")
    assert text == ""
    assert pages is None


def test_pypdf_does_not_crash_on_empty_input() -> None:
    text, pages = _extract_pypdf(b"")
    assert text == ""
    assert pages is None
