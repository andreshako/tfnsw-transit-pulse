with source as (

    select *
    from {{ source('raw', 'trip_updates_snapshot') }}

),

renamed as (

    select
        trip_id,
        route_id,
        stop_id,
        stop_sequence,
        -- TfNSW's real Sydney Trains v2 feed sets start_date on every
        -- observed trip so far, but always to "" -- not occasionally
        -- omitted, universally empty (confirmed against a full live poll,
        -- not assumed). Prefer it when it's ever actually populated;
        -- otherwise fall back to the Sydney-local calendar date of the
        -- poll itself. This is a real trade-off, not a free win: a trip
        -- polled just after midnight Sydney time could in principle still
        -- belong to the previous service day, and a poll-time fallback
        -- can't know that the way a real start_date would have. Documented
        -- as a limitation in the README rather than silently assumed away.
        coalesce(
            safe.parse_date('%Y%m%d', nullif(start_date, '')),
            date(poll_timestamp, 'Australia/Sydney')
        ) as service_date,
        arrival_delay_seconds,
        departure_delay_seconds,
        schedule_relationship,
        poll_timestamp

    from source
    where trip_id is not null
      and route_id is not null

),

-- GTFS-RT re-reports each stop's delay on every poll until the vehicle
-- passes it, so the same (trip_id, stop_id, service_date) appears many
-- times. Keep only the most recently polled observation per stop visit.
deduped as (

    select
        *,
        row_number() over (
            partition by trip_id, stop_id, service_date
            order by poll_timestamp desc
        ) as recency_rank

    from renamed

)

select
    trip_id,
    route_id,
    stop_id,
    stop_sequence,
    service_date,
    arrival_delay_seconds,
    departure_delay_seconds,
    schedule_relationship
from deduped
where recency_rank = 1
