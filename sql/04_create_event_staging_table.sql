CREATE TABLE IF NOT EXISTS staging.stg_shipment_events (
    event_id VARCHAR(50) PRIMARY KEY,
    shipment_id VARCHAR(50),
    event_type VARCHAR(50),
    event_time TIMESTAMP,
    route_id VARCHAR(50),
    warehouse_id VARCHAR(50),
    carrier_id VARCHAR(50),
    status VARCHAR(50),
    location VARCHAR(100),
    estimated_arrival TIMESTAMP,
    actual_arrival TIMESTAMP,
    delay_reason VARCHAR(255),
    ingestion_time TIMESTAMP,
    is_delayed_event BOOLEAN
);
