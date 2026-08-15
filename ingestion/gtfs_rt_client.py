"""Fetch and parse the Sydney Trains GTFS-Realtime trip-updates feed.

GTFS-RT is a live-updating feed, not a log: each stop a train hasn't yet
passed gets its predicted delay reported again on every poll. Ingestion's
job is only to capture one point-in-time snapshot per poll; deduping to the
final observed delay per stop happens later, in dbt staging.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

import requests
from google.transit import gtfs_realtime_pb2

from ingestion.config import Config

REQUEST_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class TripStopUpdate:
    poll_timestamp: datetime
    feed_timestamp: int
    trip_id: str
    route_id: str
    start_date: str | None
    stop_id: str
    stop_sequence: int
    arrival_delay_seconds: int | None
    departure_delay_seconds: int | None
    schedule_relationship: str


def fetch_feed_message(config: Config) -> gtfs_realtime_pb2.FeedMessage:
    response = requests.get(
        config.trip_updates_url,
        headers={"Authorization": f"apikey {config.tfnsw_api_key}"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def parse_trip_updates(feed: gtfs_realtime_pb2.FeedMessage) -> list[TripStopUpdate]:
    poll_timestamp = datetime.now(timezone.utc)
    rows: list[TripStopUpdate] = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        trip = trip_update.trip

        for stop_time_update in trip_update.stop_time_update:
            arrival_delay = (
                stop_time_update.arrival.delay if stop_time_update.HasField("arrival") else None
            )
            departure_delay = (
                stop_time_update.departure.delay
                if stop_time_update.HasField("departure")
                else None
            )

            rows.append(
                TripStopUpdate(
                    poll_timestamp=poll_timestamp,
                    feed_timestamp=feed.header.timestamp,
                    trip_id=trip.trip_id,
                    route_id=trip.route_id,
                    start_date=trip.start_date if trip.HasField("start_date") else None,
                    stop_id=stop_time_update.stop_id,
                    stop_sequence=stop_time_update.stop_sequence,
                    arrival_delay_seconds=arrival_delay,
                    departure_delay_seconds=departure_delay,
                    schedule_relationship=gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(
                        stop_time_update.schedule_relationship
                    ),
                )
            )

    return rows
