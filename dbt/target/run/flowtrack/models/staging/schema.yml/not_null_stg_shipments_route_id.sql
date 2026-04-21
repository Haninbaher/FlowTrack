select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select route_id
from "flowtrack"."analytics"."stg_shipments"
where route_id is null



      
    ) dbt_internal_test