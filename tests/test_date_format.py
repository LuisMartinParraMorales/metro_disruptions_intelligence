from pathlib import Path

from metro_disruptions_intelligence.processed_reader import (
    compose_path,
    discover_all_snapshot_minutes,
)


def test_discover_handles_dd_mm(tmp_path: Path) -> None:
    root = tmp_path / "rt"
    p = root / "trip_updates" / "year=2025" / "month=03" / "day=06"
    p.mkdir(parents=True, exist_ok=True)
    (p / "trip_updates_2025-06-03-11-56.parquet").touch()

    minutes = discover_all_snapshot_minutes(root)
    assert minutes

    ts = minutes[0]
    path = compose_path(ts, root, "trip_updates")
    assert path.exists()
