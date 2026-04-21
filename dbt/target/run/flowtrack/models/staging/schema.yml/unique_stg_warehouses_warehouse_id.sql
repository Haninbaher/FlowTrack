select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    warehouse_id as unique_field,
    count(*) as n_records

from "flowtrack"."analytics"."stg_warehouses"
where warehouse_id is not null
group by warehouse_id
having count(*) > 1



      
    ) dbt_internal_test