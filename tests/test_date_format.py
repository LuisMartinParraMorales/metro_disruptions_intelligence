from pathlib import Path

from metro_disruptions_intelligence.processed_reader import (
    compose_path,
    discover_all_snapshot_minutes,
)


def test_discover_handles_mm_dd(tmp_path: Path) -> None:
    root = tmp_path / "rt"
    p = root / "trip_updates" / "year=2025" / "month=05" / "day=22"
    p.mkdir(parents=True, exist_ok=True)
    (p / "trip_updates_2025-05-22-11-56.parquet").touch()

    minutes = discover_all_snapshot_minutes(root)
    assert minutes, "MM-DD file should be discovered"

    ts = minutes[0]
    # round trip via compose_path
    path = compose_path(ts, root, "trip_updates")
    assert path.exists()
