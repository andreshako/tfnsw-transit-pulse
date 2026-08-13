import dagster as dg

from orchestration.assets.dbt_assets import tfnsw_dbt_assets
from orchestration.assets.ingestion_assets import raw_trip_updates_snapshot
from orchestration.resources import dbt_resource
from orchestration.schedules import daily_mart_schedule, ingestion_schedule

defs = dg.Definitions(
    assets=[raw_trip_updates_snapshot, tfnsw_dbt_assets],
    schedules=[ingestion_schedule, daily_mart_schedule],
    resources={"dbt": dbt_resource},
)
