"""Verify feature generation on sample snapshots."""

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

from metro_disruptions_intelligence.features import SnapshotFeatureBuilder, build_route_map

route_map = build_route_map(Path("sample_data/rt_parquet"))
builder = SnapshotFeatureBuilder(route_map)

records = []
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
        local = datetime.fromtimestamp(ts, tz=pytz.UTC).astimezone(
            pytz.timezone("Australia/Sydney")
        )
        records.append({"timestamp": ts, "local_dt": local.isoformat(), "pass": ok})

df = pd.DataFrame(records)
df.to_csv("sample_data/rt_parquet/verification_report.csv", index=False)
print("Verification complete; see sample_data/rt_parquet/verification_report.csv")
