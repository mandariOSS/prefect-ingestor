"""SessionNet sub-modules — pro OParl-Entity-Typ ein Parser-Modul.

Module:
    _html       — geteilter HTTP-Client + URL-Resolver
    _dates      — Datum-/Zeit-Parsing aus deutschen Strings
    bodies      — discover_bodies (1 Body/Source aus Config)
    meetings    — list_meetings (si0040.php → si0057.php Detail)
    organizations — TODO (gr0040.php)
    persons     — TODO (kp0041.php)
    papers      — TODO (vo0040.php / do0040.php)
    files       — TODO (getfile.php Anhänge)
"""

from ingestor.adapters.sessionnet import bodies, meetings

__all__ = ["bodies", "meetings"]
