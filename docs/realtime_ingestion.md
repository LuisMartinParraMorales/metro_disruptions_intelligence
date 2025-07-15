# Realtime ingestion

The `ingest_all_rt` helper parses all GTFS-Realtime JSON files under a raw data
folder and writes them as **partitioned Parquet** files. Each feed is partitioned
by year, month and day which allows incremental processing and faster analytical
queries.

To combine the partitions into a single dataset use `union_all_feeds`. The
function streams Parquet fragments using ``pyarrow.dataset`` so memory usage
remains low even for large collections. The resulting file is written as
``data/processed/station_event.parquet``.
