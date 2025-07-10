import time

import pandas as pd

from metro_disruptions_intelligence.features import SnapshotFeatureBuilder
from metro_disruptions_intelligence.utils_gtfsrt import make_fake_vp

ROUTE_DIR_TO_STOPS_DUMMY = {("SMNW_M1", 0): [2154263]}


def make_snapshot(ts, arrival_offset):
    return pd.DataFrame({
        "snapshot_timestamp": [ts],
        "trip_id": ["T1"],
        "stop_id": [2154263],
        "direction_id": [0],
        "arrival_time": [ts + arrival_offset],
        "departure_time": [ts + arrival_offset + 30],
        "arrival_delay": [20],
        "departure_delay": [25],
        "route_id": ["SMNW_M1"],
        "stop_sequence": [10],
    })


def test_duplicate_trip_emits_one_real_row():
    builder = SnapshotFeatureBuilder(ROUTE_DIR_TO_STOPS_DUMMY)
    base = int(time.time())
    tu0 = make_snapshot(base - 60, 120).assign(trip_id="T0")
    tu1 = make_snapshot(base, 240)
    tu2 = make_snapshot(base + 60, 180)
    tu3 = make_snapshot(base + 120, 120)
    vp = make_fake_vp(base, stop_id=2154263, direction_id=0)

    builder.build_snapshot_features(tu0, vp, base - 60)

    f1 = builder.build_snapshot_features(tu1, vp, base)
    f2 = builder.build_snapshot_features(tu2, vp, base + 60)
    f3 = builder.build_snapshot_features(tu3, vp, base + 120)

    assert f1["headway_t"].notna().any()
    assert f2["headway_t"].isna().all()
    assert f3["headway_t"].isna().all()
