from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

from metro_disruptions_intelligence.utils_gtfsrt import _fname, try_parse

FEED = "trip_updates"


def _make_parquet(path: Path, ts: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"snapshot_timestamp": [ts]})
    df.to_parquet(path, index=False)


def _epoch(year, month, day, hour, minute) -> int:
    london = pytz.timezone("Europe/London")
    dt = london.localize(datetime(year, month, day, hour, minute))
    return int(dt.astimezone(pytz.UTC).timestamp())


def test_try_parse_matches_snapshot_timestamp(tmp_path: Path) -> None:
    times = [(2025, 3, 28, 12, 0), (2025, 4, 2, 12, 0), (2025, 10, 5, 12, 0)]
    for y, m, d, h, mi in times:
        ts = _epoch(y, m, d, h, mi)
        dt = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.UTC)
        fname = _fname(dt, FEED, day_first=True)
        p = tmp_path / FEED / f"year={y}" / f"month={m:02d}" / f"day={d:02d}" / fname
        _make_parquet(p, ts)
        ts_part = fname.replace(".parquet", "").split(f"{FEED}_")[-1]
        parsed = try_parse(ts_part, y, m, d)
        assert parsed is not None
        assert int(parsed.timestamp()) == ts
