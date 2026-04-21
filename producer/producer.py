import json
import time
from kafka import KafkaProducer
from generator import generate_event

TOPIC_NAME = "shipment_events"


def create_producer():
    return KafkaProducer(
        bootstrap_servers="kafka:29092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def main():
    producer = create_producer()
    print("Producer started...")

    while True:
        event = generate_event()
        producer.send(TOPIC_NAME, value=event)
        producer.flush()
        print(f"Sent event: {event}")
        time.sleep(3)


if __name__ == "__main__":
    main()
