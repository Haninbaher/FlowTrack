CREATE TABLE IF NOT EXISTS staging.stg_warehouses (
    warehouse_id VARCHAR(50) PRIMARY KEY,
    warehouse_name VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    capacity INTEGER,
    warehouse_type VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS staging.stg_carriers (
    carrier_id VARCHAR(50) PRIMARY KEY,
    carrier_name VARCHAR(100),
    carrier_type VARCHAR(50),
    service_level VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS staging.stg_routes (
    route_id VARCHAR(50) PRIMARY KEY,
    origin_warehouse_id VARCHAR(50),
    destination_warehouse_id VARCHAR(50),
    distance_km NUMERIC,
    estimated_duration_hours NUMERIC
);

CREATE TABLE IF NOT EXISTS staging.stg_shipments (
    shipment_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    route_id VARCHAR(50),
    carrier_id VARCHAR(50),
    origin_warehouse_id VARCHAR(50),
    destination_warehouse_id VARCHAR(50),
    created_at TIMESTAMP,
    promised_delivery_at TIMESTAMP,
    shipment_status VARCHAR(50),
    is_priority BOOLEAN
);
