select
    route_id,
    distance_km,
    estimated_duration_hours,

    count(*) as total_shipments,

    count(*) filter (
        where is_potentially_delayed = true
    ) as delayed_shipments,

    round(
        100.0 * count(*) filter (
            where is_potentially_delayed = true
        ) / nullif(count(*), 0),
        2
    ) as delay_rate

from {{ ref('mart_shipment_overview') }}

group by
    route_id,
    distance_km,
    estimated_duration_hours
