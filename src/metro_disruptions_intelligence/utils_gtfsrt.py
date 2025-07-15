"""Helpers for GTFS-Realtime unit tests and time conversions."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytz

_TZ_SYDNEY = pytz.timezone("Australia/Sydney")
_TZ_LONDON = pytz.timezone("Europe/London")

# Filenames use the day-first convention: YYYY-DD-MM-HH-MM
_PATTERNS = ("%Y-%d-%m-%H-%M",)

# Constants shared between features and tests
DELAY_CAP = 300
LAG_TU_SECS = 180  # trip_updates can now be up to 3 minutes behind
LAG_VP_SECS = 60  # vehicle_positions can now be up to 1 minute behind
MAX_FUTURE_SECS = 2 * 60 * 60
MAX_HEADWAY_SECS = 60 * 60

# Exported container of constants
CONSTANTS = SimpleNamespace(
    DELAY_CAP=DELAY_CAP,
    LAG_TU_SECS=LAG_TU_SECS,
    LAG_VP_SECS=LAG_VP_SECS,
    MAX_FUTURE_SECS=MAX_FUTURE_SECS,
    MAX_HEADWAY_SECS=MAX_HEADWAY_SECS,
)


def sydney_time(ts: int) -> datetime:
    """Convert epoch seconds to Sydney local time."""
    return datetime.fromtimestamp(ts, _TZ_SYDNEY)


def is_new_service_day(prev_ts: int | None, cur_ts: int, reset_at_hour: int) -> bool:
    """Return ``True`` if ``cur_ts`` starts a new service day."""
    if prev_ts is None:
        return False
    prev = sydney_time(prev_ts)
    cur = sydney_time(cur_ts)
    return (cur.date() != prev.date()) and (cur.hour >= reset_at_hour)


def make_fake_tu(
    snapshot_ts: int,
    arrival_time: int,
    *,
    stop_id: str = "STOP",
    direction_id: int = 0,
    trip_id: str = "T1",
    route_id: str = "R",
) -> pd.DataFrame:
    """Create a minimal TripUpdate DataFrame for testing."""
    return pd.DataFrame({
        "snapshot_timestamp": [snapshot_ts],
        "route_id": [route_id],
        "direction_id": [direction_id],
        "stop_id": [stop_id],
        "arrival_time": [arrival_time],
        "departure_time": [arrival_time + 30],
        "arrival_delay": [0.0],
        "departure_delay": [0.0],
        "trip_id": [trip_id],
        "stop_sequence": [1],
    })


def make_fake_vp(snapshot_ts: int, *, stop_id: str = "STOP", direction_id: int = 0) -> pd.DataFrame:
    """Create a minimal VehiclePosition DataFrame for testing."""
    return pd.DataFrame({
        "snapshot_timestamp": [snapshot_ts],
        "stop_id": [stop_id],
        "direction_id": [direction_id],
    })


def _fname(dt: datetime, feed: str, day_first: bool = True) -> str:
    """Return Parquet filename for ``feed`` at ``dt`` in London time."""
    # ``day_first`` is kept for backward compatibility but ignored; filenames are
    # always produced using the day-first convention.
    pat = "%Y-%d-%m-%H-%M"
    local = dt.astimezone(_TZ_LONDON)
    return f"{feed}_{local.strftime(pat)}.parquet"


def try_parse(ts_part: str, year: int, month: int, day: int) -> datetime | None:
    """Parse ``ts_part`` in the format ``YYYY-DD-MM-HH-MM``.

    ``ts_part`` is validated against the provided ``year``, ``month`` and
    ``day`` values. A timezone-aware UTC ``datetime`` is returned if the parsed
    timestamp matches the partition date; otherwise ``None`` is returned.
    """
    for pat in _PATTERNS:
        try:
            naive = datetime.strptime(ts_part, pat)
        except ValueError:
            continue
        if naive.year == year and naive.month == month and naive.day == day:
            local = _TZ_LONDON.localize(naive)
            return local.astimezone(pytz.UTC)
    return None
