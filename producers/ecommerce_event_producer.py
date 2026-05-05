import json
import os
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker

from config.topics import TOPICS

load_dotenv()
fake = Faker("en_IN")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
producer = Producer({"bootstrap.servers": BOOTSTRAP})

CITIES = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Pune", "Kolkata", "Chennai", "Rourkela"]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Wallet", "COD"]
FAILURE_REASONS = ["bank_timeout", "insufficient_funds", "gateway_error", "invalid_otp", "network_error"]
DELIVERY_PARTNERS = ["Delhivery", "Ecom Express", "BlueDart", "Shadowfax", "Xpressbees"]
COMPLAINTS = [
    "Payment deducted but order not confirmed",
    "Delivery is very late and support is not responding",
    "Wrong product received",
    "Refund is still pending after many days",
    "App crashed while placing order",
    "Delivery partner marked delivered but I did not receive it",
]


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")


def send(topic: str, event: dict):
    producer.produce(topic, key=event.get("order_id"), value=json.dumps(event), callback=delivery_report)
    producer.poll(0)


def make_order():
    order_id = f"ORD-{uuid4().hex[:10].upper()}"
    amount = round(random.uniform(199, 9999), 2)
    city = random.choice(CITIES)
    now = datetime.now(timezone.utc).isoformat()
    return {
        "order_id": order_id,
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "city": city,
        "order_ts": now,
        "order_amount": amount,
        "order_status": random.choice(["created", "confirmed", "packed"]),
    }


def make_payment(order):
    failed = random.random() < 0.16
    return {
        "payment_id": f"PAY-{uuid4().hex[:10].upper()}",
        "order_id": order["order_id"],
        "payment_ts": datetime.now(timezone.utc).isoformat(),
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": "failed" if failed else "success",
        "amount": order["order_amount"],
        "failure_reason": random.choice(FAILURE_REASONS) if failed else None,
    }


def make_delivery(order):
    promised = random.choice([30, 45, 60, 90])
    actual = max(10, int(random.gauss(promised, 20)))
    return {
        "delivery_id": f"DEL-{uuid4().hex[:10].upper()}",
        "order_id": order["order_id"],
        "city": order["city"],
        "delivery_partner": random.choice(DELIVERY_PARTNERS),
        "promised_minutes": promised,
        "actual_minutes": actual,
        "delivery_status": "delayed" if actual > promised else "on_time",
    }


def make_refund(order):
    return {
        "refund_id": f"REF-{uuid4().hex[:10].upper()}",
        "order_id": order["order_id"],
        "refund_ts": datetime.now(timezone.utc).isoformat(),
        "refund_amount": round(order["order_amount"] * random.uniform(0.3, 1.0), 2),
        "refund_reason": random.choice(["payment_failed", "wrong_product", "late_delivery", "customer_cancelled"]),
    }


def make_complaint(order):
    return {
        "complaint_id": f"CMP-{uuid4().hex[:10].upper()}",
        "order_id": order["order_id"],
        "complaint_ts": datetime.now(timezone.utc).isoformat(),
        "complaint_text": random.choice(COMPLAINTS),
    }


def main(events_per_second: float = 1.0):
    print("Producing live e-commerce events to Kafka...")
    while True:
        order = make_order()
        send(TOPICS["orders"], order)
        payment = make_payment(order)
        send(TOPICS["payments"], payment)
        send(TOPICS["deliveries"], make_delivery(order))

        if payment["payment_status"] == "failed" or random.random() < 0.08:
            send(TOPICS["refunds"], make_refund(order))
        if random.random() < 0.12:
            send(TOPICS["complaints"], make_complaint(order))

        producer.flush()
        print(f"Produced order {order['order_id']} amount={order['order_amount']} city={order['city']}")
        time.sleep(1 / events_per_second)


if __name__ == "__main__":
    main()
