select
    s.shipment_id,
    s.order_id,
    s.route_id,
    r.distance_km,
    r.estimated_duration_hours,
    s.carrier_id,
    c.carrier_name,
    c.carrier_type,
    c.service_level,
    s.origin_warehouse_id,
    ow.warehouse_name as origin_warehouse_name,
    s.destination_warehouse_id,
    dw.warehouse_name as destination_warehouse_name,
    s.created_at,
    s.promised_delivery_at,
    s.shipment_status,
    s.is_priority,
    case
        when s.promised_delivery_at < now() and s.shipment_status != 'DELIVERED' then true
        else false
    end as is_potentially_delayed
from "flowtrack"."analytics"."stg_shipments" s
left join "flowtrack"."analytics"."stg_routes" r
    on s.route_id = r.route_id
left join "flowtrack"."analytics"."stg_carriers" c
    on s.carrier_id = c.carrier_id
left join "flowtrack"."analytics"."stg_warehouses" ow
    on s.origin_warehouse_id = ow.warehouse_id
left join "flowtrack"."analytics"."stg_warehouses" dw
    on s.destination_warehouse_id = dw.warehouse_id