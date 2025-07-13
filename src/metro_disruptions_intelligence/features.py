"""Utilities for building station-level features from realtime snapshots."""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from pathlib import Path

import numpy as np
import pandas as pd

from .utils_gtfsrt import CONSTANTS, is_new_service_day, sydney_time

logger = logging.getLogger(__name__)


class RollingState:
    """Container for per-station rolling information."""

    def __init__(self) -> None:
        """Initialise default attributes."""
        self.last_actual_arrival: float | None = None
        self.last_sched_arrival: float | None = None
        self.last_arr_delay: float = 0.0
        self.last_dep_delay: float = 0.0
        self.last_actual_depart: float | None = None
        self.last_trip_id: str | None = None
        self.rolling_delay_5: deque[float] = deque(maxlen=5)
        self.rolling_delay_15: deque[float] = deque(maxlen=15)
        self.rolling_headway_60: deque[float] = deque(maxlen=60)
        self.last_vehicle_ts: float | None = None


class SnapshotFeatureBuilder:
    """Builder for per-minute snapshot features."""

    MAX_FUTURE_SECS = CONSTANTS.MAX_FUTURE_SECS
    MAX_HEADWAY_SECS = CONSTANTS.MAX_HEADWAY_SECS
    RESET_AT_HOUR = 3
    LAG_TU_MIN_SECS = CONSTANTS.LAG_TU_SECS
    LAG_VP_MIN_SECS = CONSTANTS.LAG_VP_SECS
    LAG_BUFFER_SECS = 10
    LAG_HISTORY_MIN = 60
    MAX_DATA_FRESH_SECS = 24 * 3600
    DELAY_CAP = CONSTANTS.DELAY_CAP

    def __init__(
        self,
        route_dir_to_stops: dict[tuple[str, int], list[str]],
        *,
        log_every: int | None = 60,
        dynamic_lag: bool = True,
    ) -> None:
        """Create the builder from a mapping of route and direction to stop lists."""
        self.route_dir_to_stops = route_dir_to_stops
        self._state: dict[tuple[str, int], RollingState] = {}
        for (_route, direction), stops in route_dir_to_stops.items():
            for stop in stops:
                self._state[(stop, direction)] = RollingState()
        self._multi_routes = False
        self._log_every = log_every
        self._build_graph()
        self._dynamic_lag = dynamic_lag
        self.LAG_TU_SECS = self.LAG_TU_MIN_SECS
        self.LAG_VP_SECS = self.LAG_VP_MIN_SECS
        self._lag_hist_tu: deque[int] = deque(maxlen=self.LAG_HISTORY_MIN)
        self._lag_hist_vp: deque[int] = deque(maxlen=self.LAG_HISTORY_MIN)

    def _build_graph(self) -> None:
        """Build node degree and hub flag graphs from the stop sequences."""
        adj: dict[str, set[str]] = defaultdict(set)
        for stops in self.route_dir_to_stops.values():
            for i, stop in enumerate(stops):
                if i > 0:
                    adj[stop].add(stops[i - 1])
                if i < len(stops) - 1:
                    adj[stop].add(stops[i + 1])
        self.node_degree = {s: len(n) for s, n in adj.items()}
        degrees = list(self.node_degree.values())
        p90 = np.percentile(degrees, 90) if degrees else 0
        self.hub_flag = {s: int(self.node_degree.get(s, 0) >= p90) for s in self.node_degree}

    def _time_features(self, ts: int) -> tuple[float, float, int]:
        """Return cyclic time-of-day features and day type."""
        t = sydney_time(ts)
        angle = 2 * np.pi * t.hour / 24
        sin_hour = np.sin(angle)
        cos_hour = np.cos(angle)
        day_type = int(t.weekday() >= 5)
        return sin_hour, cos_hour, day_type

    def _empty_feature_row(self, row: pd.Series, key: tuple[str, int]) -> dict:
        """Return a gap feature row filled with NaNs."""
        return {
            "stop_id": key[0],
            "direction_id": key[1],
            **({"route_id": row.get("route_id")} if self._multi_routes else {}),
            "arrival_delay_t": np.nan,
            "departure_delay_t": np.nan,
            "headway_t": np.nan,
            "rel_headway_t": np.nan,
            "dwell_delta_t": np.nan,
            "delay_arrival_grad_t": np.nan,
            "delay_departure_grad_t": np.nan,
            "upstream_delay_mean_2": np.nan,
            "downstream_delay_max_2": np.nan,
            "delay_mean_5": np.nan,
            "delay_std_5": np.nan,
            "delay_mean_15": np.nan,
            "headway_p90_60": np.nan,
            "sin_hour": row["sin_hour"],
            "cos_hour": row["cos_hour"],
            "day_type": row["day_type"],
            "node_degree": self.node_degree.get(key[0], 0),
            "hub_flag": self.hub_flag.get(key[0], 0),
            "is_train_present": 0,
            "data_fresh_secs": np.nan,
            "local_dt": row["local_dt"],
        }

    def build_snapshot_features(
        self, trip_updates: pd.DataFrame, vehicles: pd.DataFrame, ts: int
    ) -> pd.DataFrame:
        """Create a feature frame for one snapshot."""
        local_dt = sydney_time(ts)
        sin_hour, cos_hour, day_type = self._time_features(ts)

        logger.debug(
            "ts=%s \u2192 total TUs=%d", local_dt.strftime("%Y-%m-%d %H:%M"), len(trip_updates)
        )

        if trip_updates.empty:
            return pd.DataFrame()

        missing = set(zip(trip_updates["stop_id"], trip_updates["direction_id"])) - set(
            self._state.keys()
        )
        if missing:
            logger.warning("Found new stop/direction keys not in route_map: %s", missing)

        # ── DEBUG: log how far behind each TU message is ──────────────
        diffs = ts - trip_updates["snapshot_timestamp"]
        logger.debug(
            "snapshot %s: TU lag range = [%d, %d] sec (LAG_TU_SECS=%d)",
            local_dt.strftime("%Y-%m-%d %H:%M"),
            int(diffs.min()),
            int(diffs.max()),
            self.LAG_TU_SECS,
        )
        if self._dynamic_lag:
            self._lag_hist_tu.extend(diffs.clip(lower=0).astype(int))

        # accept every TripUpdate up to the snapshot (no lower-bound filter)
        tu_now = trip_updates[trip_updates["snapshot_timestamp"] <= ts]

        arr_time = tu_now["arrival_time"].astype(float)
        arr_diffs = arr_time - ts
        mask = (arr_time >= ts - 1) & (arr_diffs <= self.MAX_FUTURE_SECS + 1)

        dropped = (~mask).sum()
        tu_future = tu_now[mask].copy()
        logger.debug(
            " After lag removal: tu_now=%d \u2192 future_masked=%d", len(tu_now), len(tu_future)
        )
        logger.debug(
            " Future-window diffs range=[%.1f, %.1f]; dropped=%d",
            arr_diffs.min(),
            arr_diffs.max(),
            dropped,
        )
        tu_future["arrival_delay"] = tu_future["arrival_delay"].fillna(0.0)
        tu_future["departure_delay"] = tu_future["departure_delay"].fillna(0.0)

        if tu_future.empty:
            empty_rows = [
                self._empty_feature_row(
                    pd.Series({
                        "snapshot_timestamp": ts,
                        "route_id": None,
                        "sin_hour": sin_hour,
                        "cos_hour": cos_hour,
                        "day_type": day_type,
                        "local_dt": local_dt,
                    }),
                    key,
                )
                for key in self._state
            ]
            return pd.DataFrame(empty_rows)

        tu_future.sort_values("arrival_time", inplace=True)
        grouped = tu_future.groupby(["stop_id", "direction_id"], as_index=False).first()

        self._multi_routes = grouped["route_id"].nunique() > 1

        all_keys = set(self._state.keys())
        keys_with_tu = set(zip(grouped["stop_id"], grouped["direction_id"]))
        missing_keys = all_keys - keys_with_tu
        feats = []
        for key in missing_keys:
            feats.append(
                self._empty_feature_row(
                    pd.Series({
                        "route_id": None,
                        "sin_hour": sin_hour,
                        "cos_hour": cos_hour,
                        "day_type": day_type,
                        "local_dt": local_dt,
                    }),
                    key,
                )
            )

        grouped["sched_arr"] = grouped["arrival_time"] - grouped["arrival_delay"]
        grouped["sched_dep"] = grouped["departure_time"] - grouped["departure_delay"]
        grouped["dwell"] = grouped["departure_time"] - grouped["arrival_time"]
        grouped["sched_dwell"] = grouped["sched_dep"] - grouped["sched_arr"]

        if self._dynamic_lag:
            if not vehicles.empty:
                vp_diffs = ts - vehicles["snapshot_timestamp"]
                self._lag_hist_vp.extend(vp_diffs.clip(lower=0).astype(int))
            if self._lag_hist_tu:
                tu_p95 = int(np.percentile(self._lag_hist_tu, 95))
                self.LAG_TU_SECS = max(self.LAG_TU_MIN_SECS, tu_p95 + self.LAG_BUFFER_SECS)
            if self._lag_hist_vp:
                vp_p95 = int(np.percentile(self._lag_hist_vp, 95))
                self.LAG_VP_SECS = max(self.LAG_VP_MIN_SECS, vp_p95 + self.LAG_BUFFER_SECS)
            logger.debug("lag_tolerance adjusted: TU=%d VP=%d", self.LAG_TU_SECS, self.LAG_VP_SECS)

        vp_recent = vehicles[
            (vehicles["snapshot_timestamp"] <= ts)
            & (vehicles["snapshot_timestamp"] >= ts - self.LAG_VP_SECS)
        ]
        for _, row in grouped.iterrows():
            key = (row["stop_id"], int(row["direction_id"]))
            state = self._state[key]

            row["sin_hour"] = sin_hour
            row["cos_hour"] = cos_hour
            row["day_type"] = day_type
            row["local_dt"] = local_dt

            row["arrival_delay"] = np.clip(row["arrival_delay"], -self.DELAY_CAP, self.DELAY_CAP)
            row["departure_delay"] = np.clip(
                row["departure_delay"], -self.DELAY_CAP, self.DELAY_CAP
            )

            if (row["trip_id"] == state.last_trip_id) or (
                row["arrival_time"] - ts > self.MAX_FUTURE_SECS
            ):
                feats.append(self._empty_feature_row(row, key))
                continue

            if is_new_service_day(
                state.last_actual_arrival, row["arrival_time"], self.RESET_AT_HOUR
            ):
                logger.info(
                    "Service day reset for %s/%d: %s -> %s",
                    key[0],
                    key[1],
                    sydney_time(state.last_actual_arrival).strftime("%Y-%m-%d %H:%M")
                    if state.last_actual_arrival
                    else None,
                    sydney_time(row["arrival_time"]).strftime("%Y-%m-%d %H:%M"),
                )
                state.__init__()

            headway = np.nan
            rel_headway = np.nan
            sched_hw = np.nan
            dwell_delta = np.nan
            delay_arr_grad = np.nan
            delay_dep_grad = np.nan
            if state.last_actual_arrival is not None:
                headway = row["arrival_time"] - state.last_actual_arrival
                if headway <= 0 or headway > self.MAX_HEADWAY_SECS:
                    headway = np.nan
                elif state.last_sched_arrival is not None:
                    sched_hw = row["sched_arr"] - state.last_sched_arrival
                    if sched_hw:
                        rel_headway = headway / sched_hw
            dwell_delta = row["dwell"] - row["sched_dwell"]
            delay_arr_grad = row["arrival_delay"] - state.last_arr_delay
            delay_dep_grad = row["departure_delay"] - state.last_dep_delay

            # rolling stats
            rd5 = list(state.rolling_delay_5)
            rd15 = list(state.rolling_delay_15)
            rh60 = list(state.rolling_headway_60)
            delay_mean_5 = float(np.mean(rd5)) if len(rd5) == 5 else np.nan
            delay_std_5 = float(np.std(rd5, ddof=1)) if len(rd5) == 5 else np.nan
            delay_mean_15 = float(np.mean(rd15)) if len(rd15) == 15 else np.nan
            headway_p90_60 = float(np.percentile(rh60, 90)) if rh60 else np.nan

            # upstream/downstream features
            route_key = (row["route_id"], int(row["direction_id"]))
            stops = self.route_dir_to_stops.get(route_key, [])
            try:
                idx = stops.index(row["stop_id"])
            except ValueError:
                idx = -1
            upstream_delays = []
            for prev_stop in stops[max(0, idx - 2) : idx]:
                prev_state = self._state.get((prev_stop, int(row["direction_id"])))
                if prev_state and prev_state.last_arr_delay is not None:
                    upstream_delays.append(prev_state.last_arr_delay)
            upstream_delay_mean_2 = float(np.mean(upstream_delays)) if upstream_delays else np.nan
            downstream_delays = []
            next_stops = stops[idx + 1 : idx + 3]
            if not tu_future.empty:
                same_trip = tu_future[tu_future["trip_id"] == row["trip_id"]]
                for s in next_stops:
                    d = same_trip.loc[same_trip["stop_id"] == s, "arrival_delay"]
                    if not d.empty:
                        downstream_delays.append(float(d.iloc[0]))
            downstream_delay_max_2 = (
                float(np.max(downstream_delays)) if downstream_delays else np.nan
            )

            is_present = 0
            data_fresh = self.MAX_DATA_FRESH_SECS
            veh_now = vp_recent[
                (vp_recent["stop_id"] == row["stop_id"])
                & (vp_recent["direction_id"] == row["direction_id"])
            ]
            if not veh_now.empty:
                is_present = 1
                data_fresh = ts - int(veh_now["snapshot_timestamp"].max())
            elif state.last_vehicle_ts is not None:
                data_fresh = min(ts - int(state.last_vehicle_ts), self.MAX_DATA_FRESH_SECS)
            data_fresh = min(data_fresh, self.MAX_DATA_FRESH_SECS)

            feats.append({
                "stop_id": row["stop_id"],
                "direction_id": row["direction_id"],
                **({"route_id": row["route_id"]} if self._multi_routes else {}),
                "arrival_delay_t": row["arrival_delay"],
                "departure_delay_t": row["departure_delay"],
                "headway_t": headway,
                "rel_headway_t": rel_headway,
                "dwell_delta_t": dwell_delta,
                "delay_arrival_grad_t": delay_arr_grad,
                "delay_departure_grad_t": delay_dep_grad,
                "upstream_delay_mean_2": upstream_delay_mean_2,
                "downstream_delay_max_2": downstream_delay_max_2,
                "delay_mean_5": delay_mean_5,
                "delay_std_5": delay_std_5,
                "delay_mean_15": delay_mean_15,
                "headway_p90_60": headway_p90_60,
                "sin_hour": sin_hour,
                "cos_hour": cos_hour,
                "day_type": day_type,
                "node_degree": self.node_degree.get(row["stop_id"], 0),
                "hub_flag": self.hub_flag.get(row["stop_id"], 0),
                "is_train_present": is_present,
                "data_fresh_secs": data_fresh,
                "local_dt": local_dt,
            })

            # update state
            state.last_actual_arrival = row["arrival_time"]
            state.last_actual_depart = row["departure_time"]
            state.last_arr_delay = row["arrival_delay"]
            state.last_dep_delay = row["departure_delay"]
            state.last_sched_arrival = row["sched_arr"]
            state.last_trip_id = row["trip_id"]
            state.rolling_delay_5.append(row["arrival_delay"])
            state.rolling_delay_15.append(row["arrival_delay"])
            if not np.isnan(headway):
                state.rolling_headway_60.append(headway)
            if not veh_now.empty:
                state.last_vehicle_ts = veh_now["snapshot_timestamp"].max()

        df = pd.DataFrame(feats)
        df.set_index(["stop_id", "direction_id"], inplace=True)

        if self._log_every and (ts % (self._log_every * 60) == 0):
            logger.info(
                "Snapshot %s — rows=%d, headway_nan%%=%.1f, delay_nan%%=%.1f",
                local_dt.strftime("%Y-%m-%d %H:%M"),
                len(feats),
                df["headway_t"].isna().mean() * 100,
                df["arrival_delay_t"].isna().mean() * 100,
            )

        return df


def write_features(feats: pd.DataFrame, out_file: Path) -> None:
    """Write ``feats`` to ``out_file`` overwriting any existing file."""
    out_file.parent.mkdir(parents=True, exist_ok=True)
    feats.to_parquet(out_file, compression="snappy", index=False)
    logger.info("Wrote %s rows=%d", out_file, len(feats))


def build_route_map(processed_root: Path) -> dict[tuple[str, int], list[str]]:
    """Return mapping ``{(route_id, direction_id): [stop_id, ...]}``.

    All ``trip_updates`` Parquet files under ``processed_root`` are scanned and
    only the columns required to build the stop sequence are loaded. Duplicate
    rows are dropped and the stop list for each route/direction pair is sorted
    by ``stop_sequence``.
    """
    files = (processed_root / "trip_updates").rglob("trip_updates_*.parquet")
    frames = [
        pd.read_parquet(f, columns=["route_id", "direction_id", "stop_id", "stop_sequence"])
        for f in files
    ]
    if not frames:
        raise FileNotFoundError("No trip_updates*.parquet snapshots found")
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates().sort_values(["route_id", "direction_id", "stop_sequence"])
    grouped = df.groupby(["route_id", "direction_id"])["stop_id"].apply(list)
    route_map = grouped.to_dict()

    unique_stop_ids = set(df["stop_id"].unique())
    route_map_keys: set[str] = set()
    for stops in route_map.values():
        route_map_keys.update(stops)
    missing = unique_stop_ids - route_map_keys
    if missing:
        logger.warning(
            "Route map missing %d stop_ids: %s", len(missing), sorted(list(missing))[:10]
        )
    else:
        logger.debug("Route map covers all %d stop_ids", len(unique_stop_ids))

    return route_map
