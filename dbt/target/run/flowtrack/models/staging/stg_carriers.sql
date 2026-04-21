
  create view "flowtrack"."analytics"."stg_carriers__dbt_tmp"
    
    
  as (
    select
    carrier_id,
    carrier_name,
    carrier_type,
    service_level
from staging.stg_carriers
  );