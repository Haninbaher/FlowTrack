
  
    

  create  table "flowtrack"."analytics"."mart_live_shipment_status__dbt_tmp"
  
  
    as
  
  (
    with latest_event_per_shipment as (
    select
        shipment_id,
        max(event_time) as latest_event_time
    from staging.stg_shipment_events
    group by shipment_id
),

latest_events as (
    select e.*
    from staging.stg_shipment_events e
    inner join latest_event_per_shipment l
        on e.shipment_id = l.shipment_id
       and e.event_time = l.latest_event_time
)

select
    e.shipment_id,
    e.event_type as latest_event_type,
    e.status as current_status,
    e.location as current_location,
    e.event_time as last_event_time,
    e.is_delayed_event,
    e.delay_reason,
    s.route_id,
    s.carrier_id,
    c.carrier_name,
    s.origin_warehouse_id,
    s.destination_warehouse_id
from latest_events e
left join "flowtrack"."analytics"."stg_shipments" s
    on e.shipment_id = s.shipment_id
left join "flowtrack"."analytics"."stg_carriers" c
    on s.carrier_id = c.carrier_id
  );
  