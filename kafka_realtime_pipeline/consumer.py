# consumer.py
import json
from kafka import KafkaConsumer
import psycopg2


def run_consumer():
    print("[Consumer] Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host="localhost",
        port="5435",                
        dbname="kafka_db",
        user="kafka_user",
        password="kafka_password",
    )
    conn.autocommit = True
    cur = conn.cursor()
    print("[Consumer] PostgreSQL connected!")

    cur.execute("DROP TABLE IF EXISTS flight_events;")
    cur.execute(
        """
        CREATE TABLE flight_events (
            event_id SERIAL PRIMARY KEY,
            flight_id VARCHAR(10),
            airline VARCHAR(50),
            origin VARCHAR(10),
            destination VARCHAR(10),
            status VARCHAR(20),
            gate VARCHAR(10),
            departure_time TIMESTAMP,
            delay_minutes INT,
            weather VARCHAR(20),
            event_time TIMESTAMP
        );
        """
    )
    print("[Consumer] Table flight_events ready. Listening for flight events...")

    consumer = KafkaConsumer(
        "flight_events",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="flight-consumer-group",
    )

    for msg in consumer:
        event = msg.value
        print("[Consumer] Received:", event)

        try:
            cur.execute(
                """
                INSERT INTO flight_events
                (
                    flight_id, airline, origin, destination, status,
                    gate, departure_time, delay_minutes, weather, event_time
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    event["flight_id"],
                    event["airline"],
                    event["origin"],
                    event["destination"],
                    event["status"],
                    event["gate"],
                    event["departure_time"],  # ISO8601 string → TIMESTAMP
                    event["delay_minutes"],
                    event["weather"],
                    event["event_time"],
                ),
            )
        except Exception as e:
            print("[Consumer ERROR] Failed to insert:", e)


if __name__ == "__main__":
    run_consumer()
