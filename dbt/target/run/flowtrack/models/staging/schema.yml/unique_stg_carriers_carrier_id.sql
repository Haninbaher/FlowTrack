select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    carrier_id as unique_field,
    count(*) as n_records

from "flowtrack"."analytics"."stg_carriers"
where carrier_id is not null
group by carrier_id
having count(*) > 1



      
    ) dbt_internal_test