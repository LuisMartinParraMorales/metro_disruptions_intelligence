# Feature engineering workflow

This project builds per-station snapshots for Sydney Metro to detect disruptions using an IsolationForest model. The main steps are:

1. **Realtime ingestion** – Raw GTFS-Realtime JSON files are converted to partitioned Parquet datasets (`trip_updates`, `vehicle_positions`, `alerts`). Each file contains one minute of data with second resolution.
2. **Route discovery** – On first run we inspect the TripUpdate data to build `route_dir_to_stops`, a mapping from `(route_id, direction_id)` to the ordered list of stops. This allows calculation of upstream and downstream delay windows.
3. **Graph metrics** – The stop graph derived from `route_dir_to_stops` yields `node_degree` and the `hub_flag` (1 if a stop is in the 90‑th percentile of degree).
4. **Snapshot feature generation** – For every snapshot minute we call `SnapshotFeatureBuilder.build_snapshot_features`. Input TripUpdates and VehiclePositions are filtered to tolerate up to 60 s and 30 s of latency respectively. Forecasts more than two hours in the future are ignored. TripUpdates falling inside the dynamic future window are sorted by proximity to the snapshot timestamp and the closest one per (stop, direction) is used. The size of this window adjusts based on the 95‑th percentile of observed arrival-time differences.
5. **Sanity checks and state management** – The builder caps headways at one hour, resets rolling state after 03:00 local on service day changes and limits vehicle data freshness to 24 h. Invalid headways or negative intervals are discarded.
6. **Feature computation** – The following columns are produced:
   - `arrival_delay_t`, `departure_delay_t`
   - `headway_t`, `rel_headway_t`, `dwell_delta_t`
   - `delay_arrival_grad_t`, `delay_departure_grad_t`
   - `upstream_delay_mean_2`, `downstream_delay_max_2`
   - rolling statistics: `delay_mean_5`, `delay_std_5`, `delay_mean_15`, `headway_p90_60`
   - time features: `sin_hour`, `cos_hour`, `day_type`
   - network metrics: `node_degree`, `hub_flag`
   - presence indicators: `is_train_present`, `data_fresh_secs`
   A `route_id` column is included only when multiple routes appear in the snapshot.
7. **Output** – The resulting DataFrame is indexed by `(stop_id, direction_id)` and written to `data/stations_features_time_series/year=YYYY/month=MM/day=DD/stations_feats_YYYY-DD-MM-HH-MM.parquet`.

These features are then fed to an IsolationForest model to flag anomalies in real time.
