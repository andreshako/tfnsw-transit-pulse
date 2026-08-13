"""Idempotent DDL for the raw BigQuery dataset and table.

Run once (or safely re-run any time) before the first ingestion load:
    python -m ingestion.bootstrap_bigquery
"""

from google.cloud import bigquery
from google.cloud.exceptions import Conflict

from ingestion.config import Config, load_config

RAW_TABLE_NAME = "trip_updates_snapshot"

RAW_TABLE_SCHEMA = [
    bigquery.SchemaField("poll_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("feed_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("trip_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("route_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("start_date", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("stop_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("stop_sequence", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("arrival_delay_seconds", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("departure_delay_seconds", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("schedule_relationship", "STRING", mode="NULLABLE"),
]


def bootstrap(config: Config) -> None:
    client = bigquery.Client(project=config.gcp_project_id)

    dataset_ref = bigquery.DatasetReference(config.gcp_project_id, config.bq_raw_dataset)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = config.bq_location
    try:
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_ref}")
    except Conflict:
        print(f"Dataset {dataset_ref} already exists, skipping")

    table_ref = dataset_ref.table(RAW_TABLE_NAME)
    table = bigquery.Table(table_ref, schema=RAW_TABLE_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="poll_timestamp",
    )
    table.clustering_fields = ["route_id", "trip_id"]
    try:
        client.create_table(table)
        print(f"Created table {table_ref}")
    except Conflict:
        print(f"Table {table_ref} already exists, skipping")


if __name__ == "__main__":
    bootstrap(load_config())
