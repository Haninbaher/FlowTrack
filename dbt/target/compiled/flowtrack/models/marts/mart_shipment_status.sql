select
    shipment_status,
    count(*) as shipment_count
from "flowtrack"."analytics"."mart_shipment_overview"
group by shipment_status