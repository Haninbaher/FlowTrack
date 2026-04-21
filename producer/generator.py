from datetime import datetime, timedelta
import random
import uuid

SHIPMENT_TEMPLATES = [
    {
        "route_id": "R001",
        "origin_warehouse_id": "W001",
        "destination_warehouse_id": "W002",
        "carrier_id": "C002",
    },
    {
        "route_id": "R002",
        "origin_warehouse_id": "W001",
        "destination_warehouse_id": "W003",
        "carrier_id": "C001",
    },
    {
        "route_id": "R003",
        "origin_warehouse_id": "W002",
        "destination_warehouse_id": "W003",
        "carrier_id": "C003",
    },
]

WAREHOUSE_LOCATIONS = {
    "W001": "Cairo Hub",
    "W002": "Alex Hub",
    "W003": "Giza Storage",
}

LIFECYCLE = [
    ("shipment_created", "CREATED"),
    ("picked_up", "PICKED_UP"),
    ("in_transit", "IN_TRANSIT"),
    ("arrived_at_hub", "AT_HUB"),
    ("out_for_delivery", "OUT_FOR_DELIVERY"),
    ("delivered", "DELIVERED"),
]

shipment_state = {}


def create_new_shipment():
    shipment_id = f"S-{uuid.uuid4().hex[:8]}"
    template = random.choice(SHIPMENT_TEMPLATES)

    shipment_state[shipment_id] = {
        "step_index": 0,
        "last_event_time": datetime.utcnow() - timedelta(minutes=random.randint(5, 30)),
        "is_completed": False,
        "route_id": template["route_id"],
        "origin_warehouse_id": template["origin_warehouse_id"],
        "destination_warehouse_id": template["destination_warehouse_id"],
        "carrier_id": template["carrier_id"],
    }

    return shipment_id


def build_event(
    shipment_id: str,
    event_type: str,
    status: str,
    event_time: datetime,
    delay_reason=None,
):
    shipment = shipment_state[shipment_id]

    warehouse_id = shipment["origin_warehouse_id"]

    if event_type in {"arrived_at_hub", "out_for_delivery", "delivered"}:
        warehouse_id = shipment["destination_warehouse_id"]

    actual_arrival = event_time if event_type == "delivered" else None
    estimated_arrival = event_time + timedelta(hours=random.randint(2, 12))

    return {
        "event_id": str(uuid.uuid4()),
        "shipment_id": shipment_id,
        "event_type": event_type,
        "event_time": event_time.isoformat(),
        "route_id": shipment["route_id"],
        "warehouse_id": warehouse_id,
        "carrier_id": shipment["carrier_id"],
        "status": status,
        "location": WAREHOUSE_LOCATIONS.get(warehouse_id, "Unknown"),
        "estimated_arrival": estimated_arrival.isoformat(),
        "actual_arrival": actual_arrival.isoformat() if actual_arrival else None,
        "delay_reason": delay_reason,
    }


def generate_event():
    # كل شوية نضيف شحنة جديدة للنظام
    if len(shipment_state) < 5 or random.random() < 0.3:
        create_new_shipment()

    available_shipments = [
        shipment_id
        for shipment_id, state in shipment_state.items()
        if not state["is_completed"]
    ]

    if not available_shipments:
        shipment_id = create_new_shipment()
    else:
        shipment_id = random.choice(available_shipments)

    state = shipment_state[shipment_id]
    base_time = state["last_event_time"] + timedelta(minutes=random.randint(5, 45))

    # احتمال delay قبل المرحلة التالية
    if state["step_index"] > 0 and state["step_index"] < len(LIFECYCLE) - 1 and random.random() < 0.2:
        delay_reason = random.choice([
            "traffic",
            "weather",
            "warehouse backlog",
            "vehicle issue",
        ])
        state["last_event_time"] = base_time

        return build_event(
            shipment_id=shipment_id,
            event_type="delayed",
            status="DELAYED",
            event_time=base_time,
            delay_reason=delay_reason,
        )

    event_type, status = LIFECYCLE[state["step_index"]]

    event = build_event(
        shipment_id=shipment_id,
        event_type=event_type,
        status=status,
        event_time=base_time,
    )

    state["last_event_time"] = base_time

    if state["step_index"] < len(LIFECYCLE) - 1:
        state["step_index"] += 1
    else:
        state["is_completed"] = True

    return event
