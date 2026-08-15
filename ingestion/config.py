"""Environment-driven configuration for the ingestion package."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

# TfNSW deprecated the v1 Sydney Trains trip-updates endpoint on 2025-05-27 in
# favour of a v2 feed. The exact v2 path is only shown on your TfNSW Open Data
# Hub dashboard once you subscribe to the "Public Transport - Realtime Trip
# Update v2" product, so confirm/override this via TFNSW_TRIP_UPDATES_URL in
# .env rather than trusting this default blindly.
DEFAULT_TRIP_UPDATES_URL = "https://api.transport.nsw.gov.au/v2/gtfs/realtime/sydneytrains"


@dataclass(frozen=True)
class Config:
    tfnsw_api_key: str
    trip_updates_url: str
    gcp_project_id: str
    bq_raw_dataset: str
    bq_location: str


def load_config() -> Config:
    api_key = os.environ.get("TFNSW_API_KEY", "")
    if not api_key:
        raise RuntimeError(
            "TFNSW_API_KEY is not set. Copy .env.example to .env and fill it in."
        )

    project_id = os.environ.get("GCP_PROJECT_ID", "")
    if not project_id:
        raise RuntimeError(
            "GCP_PROJECT_ID is not set. Copy .env.example to .env and fill it in."
        )

    return Config(
        tfnsw_api_key=api_key,
        trip_updates_url=os.environ.get("TFNSW_TRIP_UPDATES_URL") or DEFAULT_TRIP_UPDATES_URL,
        gcp_project_id=project_id,
        bq_raw_dataset=os.environ.get("BQ_RAW_DATASET") or "raw",
        bq_location=os.environ.get("BQ_LOCATION") or "australia-southeast1",
    )
