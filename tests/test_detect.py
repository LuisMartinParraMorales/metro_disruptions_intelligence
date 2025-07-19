from pathlib import Path

import pandas as pd

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
