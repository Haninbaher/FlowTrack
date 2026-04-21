select
    route_id,
    origin_warehouse_id,
    destination_warehouse_id,
    distance_km,
    estimated_duration_hours
from staging.stg_routes
