import time
import json
import random
from datetime import datetime, timedelta

from kafka import KafkaProducer

AIRLINES = ["Delta", "American", "United", "Alaska", "Southwest", "JetBlue"]
AIRPORTS = ["SEA", "SFO", "LAX", "JFK", "ATL", "ORD", "PHX", "BOS", "MSP", "PDX"]
STATUSES = ["On Time", "Delayed", "Cancelled", "Boarding", "Departed"]
WEATHER = ["Clear", "Cloudy", "Rain", "Storm", "Snow"]
GATES = [f"{l}{n}" for l in "ABCDES" for n in range(1, 15)]


def generate_flight_event() -> dict:
    """Generate a synthetic flight status event."""
    airline = random.choice(AIRLINES)
    origin, destination = random.sample(AIRPORTS, 2)
    status = random.choice(STATUSES)
    gate = random.choice(GATES)
    weather = random.choice(WEATHER)

    now = datetime.utcnow()
    dep_time = now + timedelta(minutes=random.randint(0, 180))

    delay = random.choice([0, 0, 0, 5, 10, 20, 30, 45, 60])

    event = {
        "flight_id": f"{airline[:2].upper()}{random.randint(100, 9999)}",
        "airline": airline,
        "origin": origin,
        "destination": destination,
        "status": status,
        "gate": gate,
        "departure_time": dep_time.isoformat(),  
        "delay_minutes": delay,
        "weather": weather,
        "event_time": now.isoformat(),            
    }
    return event


def run_producer():
    print("[Producer] Connecting to Kafka at localhost:9092 ...")
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )
    print("[Producer] Connected to Kafka.")

    i = 0
    while True:
        event = generate_flight_event()
        print(f"[Producer] Sending event #{i}: {event}")
        producer.send("flight_events", value=event)
        producer.flush()
        i += 1

 
        time.sleep(random.uniform(0.5, 2.0))


if __name__ == "__main__":
    run_producer()
