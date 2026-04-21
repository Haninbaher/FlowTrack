
  create view "flowtrack"."analytics"."stg_shipments__dbt_tmp"
    
    
  as (
    select
    shipment_id,
    order_id,
    route_id,
    carrier_id,
    origin_warehouse_id,
    destination_warehouse_id,
    created_at,
    promised_delivery_at,
    shipment_status,
    is_priority
from staging.stg_shipments
  );