
  
    

  create  table "flowtrack"."analytics"."mart_delay_events__dbt_tmp"
  
  
    as
  
  (
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
from "flowtrack"."analytics"."stg_shipment_events"
where event_type = 'delayed'
  );
  