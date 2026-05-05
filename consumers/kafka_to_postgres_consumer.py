import json
import os
from datetime import datetime

import psycopg2
from confluent_kafka import Consumer
from dotenv import load_dotenv

from ai.complaint_classifier import classify_complaint
from config.topics import TOPICS

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=os.getenv("POSTGRES_PORT", "5433"),
        dbname=os.getenv("POSTGRES_DB", "ecommerce_ops"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


def parse_ts(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def log(conn, component, status, message):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_logs(component, status, message)
            VALUES (%s, %s, %s)
            """,
            (component, status, message),
        )
    conn.commit()


def insert_event(conn, topic, event):
    with conn.cursor() as cur:

        if topic == TOPICS["orders"]:
            cur.execute(
                """
                INSERT INTO raw_orders
                (order_id, customer_id, city, order_ts, order_amount, order_status)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
                """,
                (
                    event["order_id"],
                    event["customer_id"],
                    event["city"],
                    parse_ts(event["order_ts"]),
                    event["order_amount"],
                    event["order_status"],
                ),
            )

        elif topic == TOPICS["payments"]:
            cur.execute(
                """
                INSERT INTO raw_payments
                (payment_id, order_id, payment_ts, payment_method, payment_status, amount, failure_reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (payment_id) DO NOTHING
                """,
                (
                    event["payment_id"],
                    event["order_id"],
                    parse_ts(event["payment_ts"]),
                    event["payment_method"],
                    event["payment_status"],
                    event["amount"],
                    event.get("failure_reason"),
                ),
            )

        elif topic == TOPICS["deliveries"]:
            cur.execute(
                """
                INSERT INTO raw_deliveries
                (delivery_id, order_id, city, delivery_partner, promised_minutes, actual_minutes, delivery_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (delivery_id) DO NOTHING
                """,
                (
                    event["delivery_id"],
                    event["order_id"],
                    event["city"],
                    event["delivery_partner"],
                    event["promised_minutes"],
                    event["actual_minutes"],
                    event["delivery_status"],
                ),
            )

        elif topic == TOPICS["refunds"]:
            cur.execute(
                """
                INSERT INTO raw_refunds
                (refund_id, order_id, refund_ts, refund_amount, refund_reason)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (refund_id) DO NOTHING
                """,
                (
                    event["refund_id"],
                    event["order_id"],
                    parse_ts(event["refund_ts"]),
                    event["refund_amount"],
                    event["refund_reason"],
                ),
            )

        elif topic == TOPICS["complaints"]:
            ai_result = classify_complaint(event["complaint_text"])

            cur.execute(
                """
                INSERT INTO raw_complaints
                (
                    complaint_id,
                    order_id,
                    complaint_ts,
                    complaint_text,
                    ai_category,
                    ai_severity,
                    customer_emotion,
                    business_impact,
                    recommended_action
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (complaint_id) DO NOTHING
                """,
                (
                    event["complaint_id"],
                    event["order_id"],
                    parse_ts(event["complaint_ts"]),
                    event["complaint_text"],
                    ai_result.get("category"),
                    ai_result.get("severity"),
                    ai_result.get("customer_emotion"),
                    ai_result.get("business_impact"),
                    ai_result.get("recommended_action"),
                ),
            )

    conn.commit()


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "ecommerce-ops-consumer",
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe(list(TOPICS.values()))
    conn = get_conn()

    print("Consuming Kafka events into PostgreSQL...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                log(conn, "kafka_consumer", "error", str(msg.error()))
                continue

            event = json.loads(msg.value().decode("utf-8"))
            insert_event(conn, msg.topic(), event)

            print(f"Inserted {msg.topic()} event")

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()