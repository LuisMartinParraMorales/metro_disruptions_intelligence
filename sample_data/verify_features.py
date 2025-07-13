"""Verify feature generation on sample snapshots."""

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

from metro_disruptions_intelligence.features import SnapshotFeatureBuilder, build_route_map

route_map = build_route_map(Path("sample_data/rt_parquet"))
builder = SnapshotFeatureBuilder(route_map)

results = []
for month in ["04", "05"]:
    tu_dir = Path(f"sample_data/rt_parquet/trip_updates/year=2025/month={month}/day=06")
    for f in sorted(tu_dir.glob("trip_updates_*.parquet")):
        tu = pd.read_parquet(f)
        ts = int(tu["snapshot_timestamp"].iloc[0])
        vp_file = Path(str(f).replace("trip_updates", "vehicle_positions"))
        if not vp_file.exists():
            continue
        vp = pd.read_parquet(vp_file)
        feats = builder.build_snapshot_features(tu, vp, ts)
        ok = feats["headway_t"].notna().any()
        results.append((ts, ok))

with open("sample_data/verify_features_output.log", "w") as out:
    for ts, ok in results:
        dt = datetime.fromtimestamp(ts, tz=pytz.UTC).astimezone(pytz.timezone("Australia/Sydney"))
        out.write(f"{dt.isoformat()}: {'PASS' if ok else 'FAIL'}\n")
print("Verification complete; see sample_data/verify_features_output.log")
