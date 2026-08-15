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
        parse_date('%Y%m%d', start_date) as service_date,
        arrival_delay_seconds,
        departure_delay_seconds,
        schedule_relationship,
        poll_timestamp

    from source
    -- start_date is optional on the GTFS-RT trip descriptor: TfNSW omits it
    -- for some real trips (confirmed against live data, not hypothetical),
    -- and an unset optional string comes through as "" rather than NULL --
    -- guard against both, or PARSE_DATE errors out on the empty string.
    where trip_id is not null
      and route_id is not null
      and start_date is not null
      and start_date != ''

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
