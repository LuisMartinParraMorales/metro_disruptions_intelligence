from pathlib import Path

from metro_disruptions_intelligence.processed_reader import (
    compose_path,
    discover_all_snapshot_minutes,
)


def _touch(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.touch()


def test_discover_single_filename_format(tmp_path: Path) -> None:
    root = tmp_path / "rt"
    p = (
        root
        / "trip_updates"
        / "year=2025"
        / "month=03"
        / "day=06"
        / "trip_updates_2025-06-03-11-56.parquet"
    )
    _touch(p)

    minutes = discover_all_snapshot_minutes(root)
    assert len(minutes) == 1

    for ts in minutes:
        path = compose_path(ts, root, "trip_updates")
        assert path.exists()
