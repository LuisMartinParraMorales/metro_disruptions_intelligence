from pathlib import Path

from metro_disruptions_intelligence.processed_reader import (
    compose_path,
    discover_all_snapshot_minutes,
)


def _touch(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.touch()


def test_discover_both_filename_formats(tmp_path: Path) -> None:
    root = tmp_path / "rt"
    # day-month format
    p1 = (
        root
        / "trip_updates"
        / "year=2025"
        / "month=03"
        / "day=06"
        / "trip_updates_2025-06-03-11-56.parquet"
    )
    # month-day format
    p2 = (
        root
        / "trip_updates"
        / "year=2025"
        / "month=05"
        / "day=22"
        / "trip_updates_2025-05-22-11-55.parquet"
    )
    _touch(p1)
    _touch(p2)

    minutes = discover_all_snapshot_minutes(root)
    assert len(minutes) == 2

    for ts in minutes:
        path = compose_path(ts, root, "trip_updates")
        assert path.exists()
