
    
    

with all_values as (

    select
        shipment_status as value_field,
        count(*) as n_records

    from "flowtrack"."analytics"."stg_shipments"
    group by shipment_status

)

select *
from all_values
where value_field not in (
    'CREATED','IN_TRANSIT','DELIVERED','DELAYED','AT_HUB','OUT_FOR_DELIVERY'
)


