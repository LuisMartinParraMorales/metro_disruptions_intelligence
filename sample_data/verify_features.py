"""Verify feature generation on sample snapshots."""

from csv import DictWriter
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

from metro_disruptions_intelligence.features import SnapshotFeatureBuilder, build_route_map

root = Path("sample_data/rt_parquet")
route_map = build_route_map(root)
builder = SnapshotFeatureBuilder(route_map)

results = []
for month in ["04", "05"]:
    tu_dir = root / "trip_updates" / "year=2025" / f"month={month}" / "day=06"
    for f in sorted(tu_dir.glob("trip_updates_*.parquet"))[:10]:
        tu = pd.read_parquet(f)
        ts = int(tu["snapshot_timestamp"].iloc[0])
        vp_path = Path(str(f).replace("trip_updates", "vehicle_positions"))
        if vp_path.exists():
            vp = pd.read_parquet(vp_path)
        else:
            vp = pd.DataFrame(columns=["snapshot_timestamp", "stop_id", "direction_id"])
        feats = builder.build_snapshot_features(tu, vp, ts)
        ok = feats["headway_t"].notna().any()
        results.append({"timestamp": ts, "ok": ok})

report_path = root / "verification_report.csv"
with report_path.open("w", newline="") as out:
    writer = DictWriter(out, fieldnames=["timestamp", "result"])
    writer.writeheader()
    for rec in results:
        dt = datetime.fromtimestamp(rec["timestamp"], tz=pytz.UTC).astimezone(
            pytz.timezone("Australia/Sydney")
        )
        writer.writerow({"timestamp": dt.isoformat(), "result": "PASS" if rec["ok"] else "FAIL"})

print(f"Verification complete; see {report_path}")
