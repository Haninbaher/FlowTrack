select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select warehouse_id
from "flowtrack"."analytics"."stg_warehouses"
where warehouse_id is null



      
    ) dbt_internal_test