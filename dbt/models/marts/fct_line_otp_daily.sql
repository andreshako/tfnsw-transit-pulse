{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'service_date', 'data_type': 'date'},
        cluster_by=['route_short_name'],
        on_schema_change='sync_all_columns'
    )
}}

with trip_updates as (

    select *
    from {{ ref('stg_trip_updates') }}

    {% if is_incremental() %}
    -- Reprocess the last 2 days, not just "today": snapshots from late in a
    -- service day can land after midnight (pipeline catch-up, feed lag), so
    -- a narrower window would silently under-count the prior day.
    where service_date >= date_sub(current_date(), interval 2 day)
    {% endif %}

),

routes as (

    select *
    from {{ ref('seed_sydney_trains_routes') }}

),

-- Inner join: this fact table's scope is Sydney Trains lines only, so a
-- route_id with no match in the seed (e.g. a line added since the seed was
-- last refreshed) is dropped here rather than silently bucketed as unknown.
stop_events as (

    select
        trip_updates.service_date,
        routes.route_short_name,
        routes.route_long_name,
        coalesce(
            trip_updates.arrival_delay_seconds,
            trip_updates.departure_delay_seconds
        ) as effective_delay_seconds

    from trip_updates
    inner join routes
        on trip_updates.route_id = routes.route_id

),

scored as (

    select
        *,
        case
            when effective_delay_seconds is null then null
            when abs(effective_delay_seconds) <= 300 then true
            else false
        end as is_on_time

    from stop_events

)

select
    service_date,
    route_short_name,
    any_value(route_long_name) as route_long_name,
    count(*) as total_stop_events,
    countif(is_on_time) as on_time_stop_events,
    countif(is_on_time = false) as late_stop_events,
    round(avg(effective_delay_seconds) / 60.0, 2) as avg_delay_minutes,
    round(safe_divide(countif(is_on_time), countif(is_on_time is not null)), 4) as pct_on_time

from scored
group by service_date, route_short_name
