"""The upstream Python asset: one GTFS-RT poll appended to BigQuery.

Its asset key ("raw", "trip_updates_snapshot") matches the dagster meta key
declared on the dbt source in _staging__sources.yml, so stg_trip_updates
shows a real dependency edge on this asset instead of appearing as a
disconnected root in the Dagster UI.
"""

import dagster as dg

from ingestion.config import load_config
from ingestion.load_raw import load_snapshot


@dg.asset(
    key=dg.AssetKey(["raw", "trip_updates_snapshot"]),
    group_name="ingestion",
    description=(
        "One poll of the Sydney Trains GTFS-Realtime trip-updates feed, "
        "appended to BigQuery."
    ),
)
def raw_trip_updates_snapshot() -> dg.MaterializeResult:
    config = load_config()
    row_count = load_snapshot(config)
    return dg.MaterializeResult(metadata={"rows_loaded": row_count})
