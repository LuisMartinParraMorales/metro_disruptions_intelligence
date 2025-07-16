from pathlib import Path

from metro_disruptions_intelligence.etl.parse_alerts import ALERT_COLUMNS, parse_one_alert_file
from metro_disruptions_intelligence.etl.parse_trip_updates import (
    TRIP_UPDATE_COLUMNS,
    parse_one_trip_update_file,
)
from metro_disruptions_intelligence.etl.parse_vehicle_positions import (
    VEHICLE_POSITION_COLUMNS,
    parse_one_vehicle_position_file,
)

EMPTY_JSON = '{"header": {"timestamp": 0}, "entity": []}'


def _write_empty(path: Path) -> None:
    path.write_text(EMPTY_JSON, encoding="utf-8")


def test_empty_trip_update(tmp_path: Path) -> None:
    file = tmp_path / "tu.json"
    _write_empty(file)
    df = parse_one_trip_update_file(file)
    assert df.empty
    assert list(df.columns) == TRIP_UPDATE_COLUMNS


def test_empty_vehicle_position(tmp_path: Path) -> None:
    file = tmp_path / "vp.json"
    _write_empty(file)
    df = parse_one_vehicle_position_file(file)
    assert df.empty
    assert list(df.columns) == VEHICLE_POSITION_COLUMNS


def test_empty_alert(tmp_path: Path) -> None:
    file = tmp_path / "alert.json"
    _write_empty(file)
    df = parse_one_alert_file(file)
    assert df.empty
    assert list(df.columns) == ALERT_COLUMNS
