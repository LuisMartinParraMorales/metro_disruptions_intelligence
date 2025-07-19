from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
import yaml
from click.testing import CliRunner

from metro_disruptions_intelligence import cli
from metro_disruptions_intelligence.detect.streaming_iforest import (
    IForestConfig,
    StreamingIForestDetector,
)


def make_df(ts: int, stop_ids: list[str]) -> pd.DataFrame:
    data = {
        "snapshot_timestamp": [ts] * len(stop_ids),
        "stop_id": stop_ids,
        "direction_id": [0] * len(stop_ids),
        "node_degree": [1] * len(stop_ids),
        "hub_flag": [0] * len(stop_ids),
        "central_flag": [0] * len(stop_ids),
        "congestion_level": [1] * len(stop_ids),
        "occupancy": [2] * len(stop_ids),
    }
    return pd.DataFrame(data)


def test_filter_bad_stops() -> None:
    det = StreamingIForestDetector(IForestConfig())
    df = make_df(0, ["1", "204472", "2"])
    out = det.score_and_update(df)
    assert set(out["stop_id"]) == {"1", "2"}


def test_all_zero_skipped() -> None:
    det = StreamingIForestDetector(IForestConfig())
    df = make_df(0, ["1"])
    df.loc[:, ["node_degree", "hub_flag", "central_flag", "congestion_level", "occupancy"]] = 0
    out = det.score_and_update(df)
    assert out.empty


def test_warmup_no_alerts() -> None:
    cfg = IForestConfig(window_size=10)
    det = StreamingIForestDetector(cfg)
    df = make_df(0, ["1"] * 10)
    out = det.score_and_update(df)
    assert out["anomaly_flag"].sum() == 0


def test_service_day_reset() -> None:
    cfg = IForestConfig(window_size=5)
    det = StreamingIForestDetector(cfg)
    df1 = make_df(1714665600, ["1"])  # 2024-05-03 02:00 UTC -> 12:00 Sydney
    det.score_and_update(df1)
    assert det.n_obs == 1
    df2 = make_df(1714669800, ["1"])  # later same day 03:00 UTC -> 13:00 Sydney
    det.score_and_update(df2)
    assert det.n_obs == 1
    df3 = make_df(1714755900, ["1"])  # next day 03:00 Sydney
    det.score_and_update(df3)
    assert det.n_obs == 1


def test_save_load_roundtrip(tmp_path: Path) -> None:
    cfg = IForestConfig(window_size=5)
    det = StreamingIForestDetector(cfg)
    df = make_df(0, ["1"])
    det.score_and_update(df)
    p = tmp_path / "model.pkl"
    det.save(p)
    det2 = StreamingIForestDetector.load(p)
    df2 = make_df(60, ["1"])
    out1 = det.score_and_update(df2)
    out2 = det2.score_and_update(df2)
    pd.testing.assert_frame_equal(out1, out2)


def test_detect_anomalies_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli.cli, ["detect-anomalies", "--help"])
    assert result.exit_code == 0
    assert "--processed-root" in result.output


def test_tune_iforest_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli.cli, ["tune-iforest", "--help"])
    assert result.exit_code == 0
    assert "--processed-root" in result.output


def test_tune_iforest_runs(tmp_path: Path) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        processed_root = Path("features")
        start_ts = 0
        for ts in range(start_ts, start_ts + 300, 60):
            df = make_df(ts, ["S"])
            dt = datetime.fromtimestamp(ts, tz=pytz.UTC)
            out_dir = (
                processed_root
                / f"year={dt.year:04d}"
                / f"month={dt.month:02d}"
                / f"day={dt.day:02d}"
            )
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"stations_feats_{dt:%Y-%d-%m-%H-%M}.parquet"
            df.to_parquet(out_file, index=False)

        grid_yaml = Path("grid.yaml")
        grid_yaml.write_text(
            """\
n_trees: [10]
height: [8]
subsample_size: [8]
window_size: [5]
threshold_quantile: [0.97]
"""
        )

        start = datetime.utcfromtimestamp(start_ts).isoformat()
        end = datetime.utcfromtimestamp(start_ts + 300).isoformat()
        result = runner.invoke(
            cli.cli,
            [
                "tune-iforest",
                "--processed-root",
                str(processed_root),
                "--config",
                str(grid_yaml),
                "--start",
                start,
                "--end",
                end,
            ],
        )
        assert result.exit_code == 0
        res_file = Path("data/working_data/tuning_results.csv")
        assert res_file.exists()
        assert pd.read_csv(res_file).shape[0] > 0
        best_file = Path("iforest_best.yaml")
        assert best_file.exists()
        with open(best_file, encoding="utf-8") as f:
            best = yaml.safe_load(f)
        assert {
            "n_trees",
            "height",
            "subsample_size",
            "window_size",
            "threshold_quantile",
        }.issubset(best.keys())
