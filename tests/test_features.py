import pandas as pd

from metro_disruptions_intelligence.features import SnapshotFeatureBuilder


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
