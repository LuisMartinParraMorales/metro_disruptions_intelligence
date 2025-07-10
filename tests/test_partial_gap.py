import time

import pandas as pd

from metro_disruptions_intelligence.features import SnapshotFeatureBuilder
from metro_disruptions_intelligence.utils_gtfsrt import make_fake_vp

ROUTE_DIR_TO_STOPS_DUMMY = {("SMNW_M1", 0): [2154263, 2113341]}


def make_tu_row(ts, stop_id, trip_id="T1"):
    return {
        "snapshot_timestamp": ts,
        "trip_id": trip_id,
        "stop_id": stop_id,
        "direction_id": 0,
        "arrival_time": ts + 180,
        "departure_time": ts + 210,
        "arrival_delay": 30,
        "departure_delay": 35,
        "route_id": "SMNW_M1",
        "stop_sequence": 5,
    }


def test_partial_gap_row():
    builder = SnapshotFeatureBuilder(ROUTE_DIR_TO_STOPS_DUMMY)
    ts = int(time.time())

    tu = pd.DataFrame([make_tu_row(ts, 2154263), make_tu_row(ts, 2113341)])
    vp = make_fake_vp(ts, stop_id=2154263, direction_id=0)
    builder.build_snapshot_features(tu, vp, ts)

    tu2 = pd.DataFrame([make_tu_row(ts + 60, 2154263, trip_id="T2")])
    feats = builder.build_snapshot_features(tu2, vp, ts + 60)

    a = feats.loc[(2154263, 0)]
    b = feats.loc[(2113341, 0)]
    assert not pd.isna(a["arrival_delay_t"])
    assert pd.isna(b["arrival_delay_t"])
