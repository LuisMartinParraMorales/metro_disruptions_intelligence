# Anomaly detection

This guide explains how station features are used to detect unexpected network conditions using an online Isolation Forest model.

## Motivation

Timely identification of disruptions is critical for passenger information systems. We monitor per‑station snapshots and flag anomalies when the feature vectors deviate from historic patterns.

## Available features

The model consumes the following numerical inputs:

- `central_flag`
- `congestion_level`
- `occupancy`
- `node_degree`
- `hub_flag`

## Preprocessing

The raw feature files are sanitised before scoring:

1. missing values are filled with `0`
2. rows with `stop_id` in `{204472, 2155270}` are removed
3. snapshots where all numeric features are `0` are dropped
4. scaling is currently disabled (`MinMaxScaler` placeholder is commented out)

## Algorithm

A **River** `IsolationForest` is wrapped in a sliding window to maintain a fixed number of past scores. The detector skips alerting during the warm‑up period (`warmup_days = 4`). Results are written to

```text
data/anomaly_scores/year=YYYY/month=MM/day=DD/anomaly_scores_YYYY-DD-MM-HH-MM.parquet
```

## Hyper‑parameters

| name | range |
| --- | --- |
| `n_trees` | 50 – 100 |
| `height` | 8 – 10 |
| `subsample_size` | 128 – 256 |
| `window_size` | 5 000 – 10 000 |
| `threshold_quantile` | 0.97 – 0.98 |
| `warmup_days` | fixed 4 |

## Evaluation metrics

We evaluate using these metrics:

- **lead‑time ROC‑AUC** – primary score
- **precision@k** – secondary measure
- recall

## Tuning procedure

A grid of 16 parameter combinations is explored serially. Scores are cached to avoid re‑reading identical snapshots. The outputs are `tuning_results.csv` and the best configuration in `iforest_best.yaml`.

## Notes on data

Temporary stations are excluded from training and evaluation to avoid short‑term construction noise.
