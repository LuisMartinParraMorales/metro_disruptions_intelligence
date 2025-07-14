import pandas as pd

from metro_disruptions_intelligence.features import SnapshotFeatureBuilder
from metro_disruptions_intelligence.utils_gtfsrt import make_fake_tu, make_fake_vp


def _route_dir_to_stops(df: pd.DataFrame) -> dict:
    tu = df[["route_id", "direction_id", "stop_id", "stop_sequence"]].drop_duplicates()
    tu = tu.sort_values(["route_id", "direction_id", "stop_sequence"])
    return tu.groupby(["route_id", "direction_id"])["stop_id"].apply(list).to_dict()


def test_build_snapshot_features() -> None:
    sample = pd.read_parquet("sample_data/processed_sample/station_event.parquet")
    trip = sample[sample["feed_type"] == "trip_updates"]
    veh = sample[sample["feed_type"] == "vehicle_positions"]
    ts = int(trip["snapshot_timestamp"].min())
    trip_now = trip[trip["snapshot_timestamp"] == ts]
    veh_now = veh[veh["snapshot_timestamp"] == ts]
    builder = SnapshotFeatureBuilder(_route_dir_to_stops(trip))
    feats = builder.build_snapshot_features(trip_now, veh_now, ts)
    assert not feats.empty
    assert isinstance(feats.index, pd.MultiIndex)


def test_headway_bounds() -> None:
    ts = 1_000
    tu1 = make_fake_tu(ts - 30, ts + 10)
    tu2 = make_fake_tu(ts, ts + 70)
    vp = make_fake_vp(ts, stop_id="STOP")
    builder = SnapshotFeatureBuilder({("R", 0): ["STOP"]})
    builder.build_snapshot_features(tu1, vp, ts - 30)
    feats = builder.build_snapshot_features(tu2, vp, ts)
    assert feats["headway_t"].dropna().le(3600).all()


def test_snapshot_non_zero_delay() -> None:
    file = (
        "sample_data/rt_parquet/trip_updates/year=2025/month=04/day=06/"
        "trip_updates_2025-06-04-13-17.parquet"
    )
    tu = pd.read_parquet(file)
    vp = pd.read_parquet(file.replace("trip_updates", "vehicle_positions"))
    ts = int(tu["snapshot_timestamp"].iloc[0])
    builder = SnapshotFeatureBuilder(_route_dir_to_stops(tu))
    feats = builder.build_snapshot_features(tu, vp, ts).reset_index()
    assert not feats.empty
    assert (feats["arrival_delay_t"].abs() > 0).any()
