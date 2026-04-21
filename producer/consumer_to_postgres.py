import json
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text

DB_USER = "flowtrack"
DB_PASSWORD = "flowtrack"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "flowtrack"

TOPIC_NAME = "shipment_events"


def get_engine():
    conn_str = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(conn_str)


def create_consumer():
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers="kafka:29092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="flowtrack-consumer-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )


def insert_event(engine, event):
    insert_sql = text("""
        INSERT INTO raw.shipment_events (
            event_id,
            shipment_id,
            event_type,
            event_time,
            route_id,
            warehouse_id,
            carrier_id,
            status,
            location,
            estimated_arrival,
            actual_arrival,
            delay_reason
        )
        VALUES (
            :event_id,
            :shipment_id,
            :event_type,
            :event_time,
            :route_id,
            :warehouse_id,
            :carrier_id,
            :status,
            :location,
            :estimated_arrival,
            :actual_arrival,
            :delay_reason
        )
        ON CONFLICT (event_id) DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, event)


def main():
    engine = get_engine()
    consumer = create_consumer()

    print("Consumer started...")

    for message in consumer:
        event = message.value
        insert_event(engine, event)
        print(f"Inserted event: {event['event_id']} - {event['event_type']}")


if __name__ == "__main__":
    main()
