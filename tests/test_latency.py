from metro_disruptions_intelligence.features import SnapshotFeatureBuilder
from metro_disruptions_intelligence.utils_gtfsrt import make_fake_tu, make_fake_vp

ROUTE_MAP = {("R", 0): ["STOP"]}


def test_tolerates_60s_tu_lag():
    ts = 1_000
    tu = make_fake_tu(ts - 40, ts + 50)
    vp = make_fake_vp(ts, stop_id="STOP")
    builder = SnapshotFeatureBuilder(ROUTE_MAP)
    feats = builder.build_snapshot_features(tu, vp, ts)
    assert not feats.empty


def test_discards_90s_tu_lag():
    ts = 1_000
    tu = make_fake_tu(ts - 90, ts + 50)
    vp = make_fake_vp(ts, stop_id="STOP")
    builder = SnapshotFeatureBuilder(ROUTE_MAP)
    feats = builder.build_snapshot_features(tu, vp, ts)
    assert not feats.empty
    assert feats["arrival_delay_t"].notna().any()
