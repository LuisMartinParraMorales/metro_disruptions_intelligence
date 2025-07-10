"""Utilities for building station-level features from realtime snapshots."""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime

import numpy as np
import pandas as pd
import pytz

from .utils_gtfsrt import is_new_service_day


class RollingState:
    """Container for per-station rolling information."""

    def __init__(self) -> None:
        """Initialise default attributes."""
        self.last_actual_arrival: float | None = None
        self.last_sched_arrival: float | None = None
        self.last_arr_delay: float = 0.0
        self.last_dep_delay: float = 0.0
        self.last_actual_depart: float | None = None
        self.rolling_delay_5: deque[float] = deque(maxlen=5)
        self.rolling_delay_15: deque[float] = deque(maxlen=15)
        self.rolling_headway_60: deque[float] = deque(maxlen=60)
        self.last_vehicle_ts: float | None = None


class SnapshotFeatureBuilder:
    """Builder for per-minute snapshot features."""

    MAX_FUTURE_SECS = 2 * 60 * 60
    MAX_HEADWAY_SECS = 60 * 60
    RESET_AT_HOUR = 3
    LAG_TU_SECS = 60
    LAG_VP_SECS = 30
    MAX_DATA_FRESH_SECS = 24 * 3600

    def __init__(self, route_dir_to_stops: dict[tuple[str, int], list[str]]) -> None:
        """Create the builder from a mapping of route and direction to stop lists."""
        self.route_dir_to_stops = route_dir_to_stops
        self.state: dict[tuple[str, int], RollingState] = defaultdict(RollingState)
        self._build_graph()

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

    @staticmethod
    def _sydney_time(ts: int) -> datetime:
        tz = pytz.timezone("Australia/Sydney")
        return datetime.fromtimestamp(ts, tz)

    def _time_features(self, ts: int) -> tuple[float, float, int]:
        """Return cyclic time-of-day features and day type."""
        t = self._sydney_time(ts)
        angle = 2 * np.pi * t.hour / 24
        sin_hour = np.sin(angle)
        cos_hour = np.cos(angle)
        day_type = int(t.weekday() >= 5)
        return sin_hour, cos_hour, day_type

    def build_snapshot_features(
        self, trip_updates: pd.DataFrame, vehicles: pd.DataFrame, ts: int
    ) -> pd.DataFrame:
        """Create a feature frame for one snapshot."""
        sin_hour, cos_hour, day_type = self._time_features(ts)

        if trip_updates.empty:
            return pd.DataFrame()

        tu_now = trip_updates[
            (trip_updates["snapshot_timestamp"] <= ts)
            & (trip_updates["snapshot_timestamp"] >= ts - self.LAG_TU_SECS)
        ]

        mask = (tu_now["arrival_time"] >= ts) & (
            tu_now["arrival_time"] - ts <= self.MAX_FUTURE_SECS
        )

        tu_future = tu_now[mask].copy()

        if tu_future.empty:
            return pd.DataFrame()

        tu_future.sort_values("arrival_time", inplace=True)
        grouped = tu_future.groupby(["stop_id", "direction_id"], as_index=False).first()

        grouped["sched_arr"] = grouped["arrival_time"] - grouped["arrival_delay"]
        grouped["sched_dep"] = grouped["departure_time"] - grouped["departure_delay"]
        grouped["dwell"] = grouped["departure_time"] - grouped["arrival_time"]
        grouped["sched_dwell"] = grouped["sched_dep"] - grouped["sched_arr"]

        vp_recent = vehicles[
            (vehicles["snapshot_timestamp"] <= ts)
            & (vehicles["snapshot_timestamp"] >= ts - self.LAG_VP_SECS)
        ]

        feats = []
        multi_routes = grouped["route_id"].nunique() > 1
        for _, row in grouped.iterrows():
            key = (row["stop_id"], int(row["direction_id"]))
            state = self.state[key]

            if is_new_service_day(
                state.last_actual_arrival, row["arrival_time"], self.RESET_AT_HOUR
            ):
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
                prev_state = self.state.get((prev_stop, int(row["direction_id"])))
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
                **({"route_id": row["route_id"]} if multi_routes else {}),
                "arrival_delay_t": row["arrival_delay"],
                "departure_delay_t": row["departure_delay"],
                "headway_t": headway if not np.isnan(headway) else sched_hw,
                "rel_headway_t": rel_headway if not np.isnan(rel_headway) else 1.0,
                "dwell_delta_t": dwell_delta if not np.isnan(dwell_delta) else 0.0,
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
            })

            # update state
            state.last_actual_arrival = row["arrival_time"]
            state.last_actual_depart = row["departure_time"]
            state.last_arr_delay = row["arrival_delay"]
            state.last_dep_delay = row["departure_delay"]
            state.last_sched_arrival = row["sched_arr"]
            state.rolling_delay_5.append(row["arrival_delay"])
            state.rolling_delay_15.append(row["arrival_delay"])
            if not np.isnan(headway):
                state.rolling_headway_60.append(headway)
            if not veh_now.empty:
                state.last_vehicle_ts = veh_now["snapshot_timestamp"].max()

        df = pd.DataFrame(feats)
        df.set_index(["stop_id", "direction_id"], inplace=True)
        return df
