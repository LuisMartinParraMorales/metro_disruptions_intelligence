from pathlib import Path

import pandas as pd

from metro_disruptions_intelligence.etl.ingest_rt import _parse_args as parse_ingest_args
from metro_disruptions_intelligence.etl.replay_stream import _parse_args as parse_replay_args
from metro_disruptions_intelligence.etl.static_ingest import _parse_args as parse_static_args
from metro_disruptions_intelligence.etl.write_parquet import write_df_to_partitioned_parquet


def test_ingest_rt_parse_args(tmp_path):
    cfg = parse_ingest_args([
        str(Path("sample_data/rt")),
        "--processed-root",
        str(tmp_path),
        "--union",
    ])
    assert cfg.raw_root == Path("sample_data/rt")
    assert cfg.processed_root == tmp_path
    assert cfg.union is True


def test_static_ingest_parse_args(tmp_path):
    cfg = parse_static_args([
        "sample_data/static",
        "--output-dir",
        str(tmp_path),
        "--persist-duckdb",
    ])
    assert cfg.gtfs_dir == Path("sample_data/static")
    assert cfg.output_dir == tmp_path
    assert cfg.persist_duckdb is True


def test_replay_stream_parse_args(tmp_path):
    dummy = tmp_path / "dummy.parquet"
    cfg = parse_replay_args([str(dummy), "--batch-size", "10", "--start-ts", "1", "--end-ts", "2"])
    assert cfg.path == dummy
    assert cfg.batch_size == 10
    assert cfg.start_ts == 1
    assert cfg.end_ts == 2


def test_write_df_to_partitioned_parquet(tmp_path):
    df = pd.DataFrame({"snapshot_timestamp": [100], "value": [1]})
    out = write_df_to_partitioned_parquet(df, tmp_path, "test")
    assert out is not None
    assert out.exists()


def test_ingest_all_rt_pattern(tmp_path):
    """Ingest files when feeds are in the top-level directory."""
    from metro_disruptions_intelligence.etl.ingest_rt import ingest_all_rt

    src_root = tmp_path / "raw"
    src_root.mkdir()
    mapping = {
        "foo_alert.json": "sample_alert.json",
        "bar_trip_update.json": "sample_trip_update.json",
        "baz_vehicle_position.json": "sample_vehicles_position.json",
    }
    for fname, src in mapping.items():
        sample = Path("sample_data/rt") / src
        (src_root / fname).write_text(sample.read_text(), encoding="utf-8")

    ingest_all_rt(src_root, tmp_path)
    for feed in ["alerts", "trip_updates", "vehicle_positions"]:
        files = list((tmp_path / feed).rglob("*.parquet"))
        assert files


def test_ingest_rt_main(tmp_path):
    from metro_disruptions_intelligence.etl.ingest_rt import main

    main([str(Path("sample_data/rt")), "--processed-root", str(tmp_path), "--union"])
    assert (tmp_path.parent / "station_event.parquet").exists()


def test_static_ingest_main(tmp_path):
    from metro_disruptions_intelligence.etl.static_ingest import main

    main(["sample_data/static", "--output-dir", str(tmp_path)])
    assert (tmp_path / "station_schedule.parquet").exists()


def test_replay_stream_main(tmp_path, capsys):
    from metro_disruptions_intelligence.etl.ingest_rt import ingest_all_rt, union_all_feeds
    from metro_disruptions_intelligence.etl.replay_stream import main as replay_main

    ingest_all_rt(Path("sample_data/rt"), tmp_path)
    out = tmp_path.parent / "station_event.parquet"
    union_all_feeds(tmp_path, out)
    replay_main([str(out), "--batch-size", "1"])
    captured = capsys.readouterr()
    assert "snapshot_timestamp" in captured.out
