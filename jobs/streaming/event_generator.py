"""Pure-Python event generator for local demo (no Kafka required)."""
from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

_SEED = 42
_BASE_TS = datetime(2026, 1, 1, 0, 0, 0)

_EVENT_SEQUENCE = [
    "page_view",
    "page_view",
    "signup_completed",
    "page_view",
    "product_viewed",
    "product_viewed",
    "add_to_cart",
    "checkout_started",
    "payment_succeeded",
    "order_created",
    "order_shipped",
    "order_delivered",
]

_PRODUCTS = [
    ("prod_001", "Laptop Pro", 1299.99, "Electronics"),
    ("prod_002", "Wireless Earbuds", 89.99, "Electronics"),
    ("prod_003", "Running Shoes", 119.99, "Apparel"),
    ("prod_004", "Coffee Maker", 79.99, "Home"),
    ("prod_005", "Yoga Mat", 34.99, "Fitness"),
    ("prod_006", "Backpack", 59.99, "Accessories"),
    ("prod_007", "Smartwatch", 249.99, "Electronics"),
    ("prod_008", "Desk Lamp", 44.99, "Home"),
]

_CHANNELS = ["organic", "paid_search", "social", "email", "referral"]
_PAGES = ["/", "/products", "/about", "/cart", "/checkout", "/confirmation"]


def _ts(base: datetime, offset_seconds: int) -> str:
    return (base + timedelta(seconds=offset_seconds)).isoformat() + "Z"


def generate_events(n_users: int = 80, rng_seed: int = _SEED) -> list[dict]:
    rng = random.Random(rng_seed)
    events: list[dict] = []
    t = 0

    for _ in range(n_users):
        user_id = f"user_{uuid.UUID(int=rng.getrandbits(128))}"
        channel = rng.choice(_CHANNELS)
        session_id = f"sess_{uuid.UUID(int=rng.getrandbits(128))}"
        product = rng.choice(_PRODUCTS)
        depth = rng.randint(3, len(_EVENT_SEQUENCE))

        for step in _EVENT_SEQUENCE[:depth]:
            t += rng.randint(5, 120)
            event: dict = {
                "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "event_type": step,
                "user_id": user_id,
                "session_id": session_id,
                "channel": channel,
                "occurred_at": _ts(_BASE_TS, t),
                "event_version": "1.0",
                "source": "event-generator",
            }
            if step == "page_view":
                event["page"] = rng.choice(_PAGES)
            if step in ("product_viewed", "add_to_cart"):
                event["product_id"] = product[0]
                event["product_name"] = product[1]
                event["product_category"] = product[3]
            if step in ("checkout_started", "payment_succeeded", "order_created",
                        "order_shipped", "order_delivered"):
                order_id = f"ord_{uuid.UUID(int=rng.getrandbits(128))}"
                event["order_id"] = order_id
                event["amount_usd"] = round(product[2] * rng.uniform(0.9, 1.1), 2)
                event["product_id"] = product[0]
            events.append(event)

    return events


def write_bronze_ndjson(events: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        "\n".join(json.dumps(e) for e in events),
        encoding="utf-8",
    )


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    bronze_dir = root / "data" / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    events = generate_events()
    path = bronze_dir / "events.ndjson"
    write_bronze_ndjson(events, path)
    print(f"Generated {len(events)} events → {path}")
