select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select carrier_id
from "flowtrack"."analytics"."stg_carriers"
where carrier_id is null



      
    ) dbt_internal_test