"""Poll the GTFS-RT feed once and append the resulting rows to BigQuery.

Uses a load job (not the streaming insert API): at a ~10-minute polling
cadence we don't need sub-second freshness, and load jobs are free while
streaming inserts are metered and quota-limited -- the wrong trade for a
portfolio project's budget.
"""

from dataclasses import asdict
from datetime import datetime, timezone

from google.cloud import bigquery

from ingestion.bootstrap_bigquery import RAW_TABLE_NAME, RAW_TABLE_SCHEMA
from ingestion.config import Config, load_config
from ingestion.gtfs_rt_client import TripStopUpdate, fetch_feed_message, parse_trip_updates


def _to_json_row(row: TripStopUpdate) -> dict:
    payload = asdict(row)
    payload["poll_timestamp"] = row.poll_timestamp.isoformat()
    payload["feed_timestamp"] = datetime.fromtimestamp(
        row.feed_timestamp, tz=timezone.utc
    ).isoformat()
    return payload


def load_snapshot(config: Config) -> int:
    feed = fetch_feed_message(config)
    rows = parse_trip_updates(feed)

    if not rows:
        print("No trip updates in this poll; nothing to load")
        return 0

    client = bigquery.Client(project=config.gcp_project_id)
    table_ref = bigquery.DatasetReference(
        config.gcp_project_id, config.bq_raw_dataset
    ).table(RAW_TABLE_NAME)

    job_config = bigquery.LoadJobConfig(
        schema=RAW_TABLE_SCHEMA,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_json(
        [_to_json_row(row) for row in rows], table_ref, job_config=job_config
    )
    job.result()

    print(f"Loaded {len(rows)} trip stop updates into {table_ref}")
    return len(rows)


if __name__ == "__main__":
    load_snapshot(load_config())
