select
    carrier_id,
    carrier_name,
    carrier_type,
    service_level,

    count(*) as total_shipments,

    count(*) filter (
        where shipment_status = 'DELIVERED'
    ) as delivered_shipments,

    count(*) filter (
        where is_potentially_delayed = true
    ) as delayed_shipments,

    round(
        100.0 * count(*) filter (
            where shipment_status = 'DELIVERED'
        ) / nullif(count(*), 0),
        2
    ) as delivery_success_rate

from "flowtrack"."analytics"."mart_shipment_overview"

group by
    carrier_id,
    carrier_name,
    carrier_type,
    service_level