"""Parse GTFS-realtime alert JSON files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from pydantic import BaseModel


class AlertRow(BaseModel):
    snapshot_timestamp: int
    alert_entity_id: str
    active_period_start: Optional[int] = None
    active_period_end: Optional[int] = None
    agency_id: Optional[str] = None
    route_id: Optional[str] = None
    direction_id: Optional[int] = None
    cause: Optional[str] = None
    effect: Optional[str] = None
    header_text: Optional[str] = None
    description_text: Optional[str] = None
    url: Optional[str] = None


ALERT_COLUMNS = list(AlertRow.__fields__.keys())


def parse_one_alert_file(json_path: Path) -> pd.DataFrame:
    """Return a DataFrame of alerts contained in ``json_path``."""
    raw = json.loads(json_path.read_text())
    header_ts = int(raw["header"]["timestamp"])
    rows: List[Dict] = []
    for entity in raw.get("entity", []):
        alert = entity.get("alert")
        if not alert:
            continue
        for period in alert.get("active_period", [{}]):
            for ie in alert.get("informed_entity", []):
                row = AlertRow(
                    snapshot_timestamp=header_ts,
                    alert_entity_id=entity["id"],
                    active_period_start=period.get("start"),
                    active_period_end=period.get("end"),
                    agency_id=ie.get("agency_id"),
                    route_id=ie.get("route_id"),
                    direction_id=ie.get("direction_id"),
                    cause=str(alert["cause"]) if alert.get("cause") is not None else None,
                    effect=str(alert["effect"]) if alert.get("effect") is not None else None,
                    header_text=" ".join(
                        t.get("text", "")
                        for t in alert.get("header_text", {}).get("translation", [])
                    ),
                    description_text=" ".join(
                        t.get("text", "")
                        for t in alert.get("description_text", {}).get("translation", [])
                    ),
                    url=next(
                        (u.get("text") for u in alert.get("url", {}).get("translation", [])), None
                    ),
                )
                rows.append(row.dict())
    return pd.DataFrame(rows, columns=ALERT_COLUMNS)
