select
    shipment_id,
    latest_event_type,
    current_status,
    current_location,
    last_event_time,
    is_delayed_event,
    delay_reason,
    route_id,
    carrier_id,
    carrier_name,
    origin_warehouse_id,
    destination_warehouse_id
from "flowtrack"."analytics"."mart_live_shipment_status"
where current_status = 'DELAYED'