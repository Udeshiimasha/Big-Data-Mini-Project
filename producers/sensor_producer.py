# producers/sensor_producer.py
"""
Simulates 4 junction sensors producing JSON every second to Kafka topic `traffic-events`.

Occasionally generates "critical traffic" events (low avg_speed).

Requires: kafka-python (pip install kafka-python)
"""

import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9093"  # Using external listener
TOPIC = "traffic-events"
SENSOR_IDS = ["JUNCTION_A", "JUNCTION_B", "JUNCTION_C", "JUNCTION_D"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=5,
)

def generate_reading(sensor_id):
    # Simulate normal traffic distribution and occasional critical slowdowns
    # base vehicle count 0-20 per second; avg_speed 10-60 km/h
    # we occasionally (1 in 200) force a critical drop in speed
    critical = (random.randint(1, 200) == 1)
    vehicle_count = random.randint(0, 40) if critical else random.randint(0, 20)
    avg_speed = round(random.uniform(2, 8), 2) if critical else round(random.uniform(15, 60), 2)

    reading = {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "vehicle_count": vehicle_count,
        "avg_speed": avg_speed
    }
    return reading, critical

def main():
    print("Starting sensor producers to topic:", TOPIC)
    print("Sensors:", ", ".join(SENSOR_IDS))
    print("Press Ctrl+C to stop...")
    try:
        while True:
            # Produce from all sensors each second
            for sid in SENSOR_IDS:
                reading, critical = generate_reading(sid)
                producer.send(TOPIC, value=reading)
                status = "[CRITICAL]" if critical else ""
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {sid}: vehicles={reading['vehicle_count']}, speed={reading['avg_speed']} km/h {status}")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping producers.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()

