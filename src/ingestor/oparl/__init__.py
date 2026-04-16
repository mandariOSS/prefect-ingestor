"""OParl-Client + Parser."""

from ingestor.oparl.client import OParlClient
from ingestor.oparl.types import OParlType, detect_oparl_type

__all__ = ["OParlClient", "OParlType", "detect_oparl_type"]
