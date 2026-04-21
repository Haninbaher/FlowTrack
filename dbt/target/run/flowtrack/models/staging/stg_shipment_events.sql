
  create view "flowtrack"."analytics"."stg_shipment_events__dbt_tmp"
    
    
  as (
    select
    event_id,
    shipment_id,
    event_type,
    event_time,
    route_id,
    warehouse_id,
    carrier_id,
    status,
    location,
    estimated_arrival,
    actual_arrival,
    nullif(delay_reason, '') as delay_reason,
    ingestion_time,
    is_delayed_event
from staging.stg_shipment_events
  );