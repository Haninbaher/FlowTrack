select
    event_id,
    shipment_id,
    event_type,
    status,
    location,
    event_time,
    delay_reason,
    route_id,
    carrier_id,
    warehouse_id,
    ingestion_time
from {{ ref('stg_shipment_events') }}
where event_type = 'delayed'
