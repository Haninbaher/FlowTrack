select
    shipment_status,
    count(*) as shipment_count
from {{ ref('mart_shipment_overview') }}
group by shipment_status
