"""Parse GTFS-realtime trip update JSON files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from pydantic import BaseModel


class TripUpdateRow(BaseModel):
    snapshot_timestamp: int
    trip_id: str
    route_id: str
    direction_id: int
    start_time: str
    start_date: str
    vehicle_id: Optional[str]
    stop_sequence: int
    stop_id: str
    arrival_time: Optional[int] = None
    departure_time: Optional[int] = None
    arrival_delay: Optional[int] = None
    departure_delay: Optional[int] = None


def parse_one_trip_update_file(json_path: Path) -> pd.DataFrame:
    """Return all stop time updates contained in ``json_path``."""
    raw = json.loads(json_path.read_text())
    header_ts = int(raw["header"]["timestamp"])
    rows: List[Dict] = []
    for entity in raw.get("entity", []):
        tu = entity.get("trip_update")
        if not tu:
            continue
        trip = tu["trip"]
        vehicle_id = tu.get("vehicle", {}).get("id")
        for stu in tu.get("stop_time_update", []):
            row = TripUpdateRow(
                snapshot_timestamp=header_ts,
                trip_id=trip["trip_id"],
                route_id=trip["route_id"],
                direction_id=int(trip.get("direction_id", 0)),
                start_time=trip["start_time"],
                start_date=trip["start_date"],
                vehicle_id=vehicle_id,
                stop_sequence=int(stu["stop_sequence"]),
                stop_id=stu["stop_id"],
                arrival_time=stu.get("arrival", {}).get("time"),
                departure_time=stu.get("departure", {}).get("time"),
                arrival_delay=stu.get("arrival", {}).get("delay"),
                departure_delay=stu.get("departure", {}).get("delay"),
            )
            rows.append(row.dict())
    return pd.DataFrame(rows)
