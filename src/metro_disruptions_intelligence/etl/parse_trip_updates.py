"""Parse GTFS-realtime trip update JSON files."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from pydantic import BaseModel


class TripUpdateRow(BaseModel):
    """Single stop update extracted from a TripUpdate message."""

    snapshot_timestamp: int
    trip_id: str
    route_id: str
    direction_id: int
    start_time: str
    start_date: str
    vehicle_id: str | None
    stop_sequence: int
    stop_id: str
    arrival_time: int | None = None
    departure_time: int | None = None
    arrival_delay: int | None = None
    departure_delay: int | None = None


TRIP_UPDATE_COLUMNS = list(TripUpdateRow.__fields__.keys())


def parse_one_trip_update_file(json_path: Path) -> pd.DataFrame:
    """Return all stop time updates contained in ``json_path``."""
    raw = json.loads(json_path.read_text())
    header_ts = int(raw["header"]["timestamp"])
    rows: list[dict] = []
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
                arrival_delay=float(stu.get("arrival", {}).get("delay", 0.0) or 0.0),
                departure_delay=float(stu.get("departure", {}).get("delay", 0.0) or 0.0),
            )
            rows.append(row.dict())
    return pd.DataFrame(rows, columns=TRIP_UPDATE_COLUMNS)
