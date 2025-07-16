"""Parse GTFS-realtime vehicle position JSON files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from pydantic import BaseModel


class VehiclePositionRow(BaseModel):
    snapshot_timestamp: int
    trip_id: Optional[str] = None
    route_id: Optional[str] = None
    direction_id: Optional[int] = None
    vehicle_id: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    bearing: Optional[float] = None
    speed: Optional[float] = None
    current_stop_sequence: Optional[int] = None
    current_status: Optional[str] = None
    stop_id: Optional[str] = None
    congestion_level: Optional[str] = None
    occupancy_status: Optional[str] = None


VEHICLE_POSITION_COLUMNS = list(VehiclePositionRow.__fields__.keys())


def parse_one_vehicle_position_file(json_path: Path) -> pd.DataFrame:
    """Return a DataFrame of vehicle positions contained in ``json_path``."""
    raw = json.loads(json_path.read_text())
    header_ts = int(raw["header"]["timestamp"])
    rows: List[Dict] = []
    for entity in raw.get("entity", []):
        veh = entity.get("vehicle")
        if not veh:
            continue
        trip = veh.get("trip", {})
        pos = veh.get("position", {})
        row = VehiclePositionRow(
            snapshot_timestamp=header_ts,
            trip_id=trip.get("trip_id"),
            route_id=trip.get("route_id"),
            direction_id=trip.get("direction_id"),
            vehicle_id=veh.get("vehicle", {}).get("id"),
            latitude=pos.get("latitude"),
            longitude=pos.get("longitude"),
            bearing=pos.get("bearing"),
            speed=pos.get("speed"),
            current_stop_sequence=veh.get("current_stop_sequence"),
            current_status=str(veh["current_status"])
            if veh.get("current_status") is not None
            else None,
            stop_id=veh.get("stop_id"),
            congestion_level=str(veh["congestion_level"])
            if veh.get("congestion_level") is not None
            else None,
            occupancy_status=str(veh["occupancy_status"])
            if veh.get("occupancy_status") is not None
            else None,
        )
        rows.append(row.dict())
    return pd.DataFrame(rows, columns=VEHICLE_POSITION_COLUMNS)
