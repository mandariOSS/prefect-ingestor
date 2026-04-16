"""Tests für OParl-Typ-Erkennung."""

from ingestor.oparl import OParlType, detect_oparl_type


def test_from_url_oparl_11() -> None:
    assert OParlType.from_url("https://schema.oparl.org/1.1/Body") == OParlType.BODY


def test_from_url_oparl_10() -> None:
    assert OParlType.from_url("https://schema.oparl.org/1.0/Meeting") == OParlType.MEETING


def test_from_url_unknown_returns_none() -> None:
    assert OParlType.from_url("https://example.com/Foo") is None


def test_detect_from_dict() -> None:
    assert detect_oparl_type({"type": "https://schema.oparl.org/1.1/Paper"}) == OParlType.PAPER


def test_detect_empty() -> None:
    assert detect_oparl_type({}) is None
    assert detect_oparl_type({"type": ""}) is None
