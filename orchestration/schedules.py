"""Two schedules for two different jobs: keep the raw table fresh all day,
build the mart once the prior service day is fully captured.
"""

import dagster as dg

from orchestration.assets.dbt_assets import tfnsw_dbt_assets
from orchestration.assets.ingestion_assets import raw_trip_updates_snapshot

ingestion_schedule = dg.ScheduleDefinition(
    name="ingestion_schedule",
    cron_schedule="*/10 5-23 * * *",
    execution_timezone="Australia/Sydney",
    target=dg.AssetSelection.assets(raw_trip_updates_snapshot),
)

daily_mart_schedule = dg.ScheduleDefinition(
    name="daily_mart_schedule",
    cron_schedule="0 4 * * *",
    execution_timezone="Australia/Sydney",
    target=dg.AssetSelection.assets(tfnsw_dbt_assets),
)
