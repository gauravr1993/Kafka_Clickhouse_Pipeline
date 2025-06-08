import json
import time
from confluent_kafka import Producer
from config import *
import logging
from producer.sensors import generate_sensor_data
import random
# from producer.utils import fetch_sensor_data

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaProducer")

producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Simulate 1000 devices
device_ids = [f'sensor-{i:04}' for i in range(1000)]


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


def run_producer():
    print("[INFO] Producer running...")
    while True:
        # data = fetch_sensor_data(INFLUX_API_URL)
        for device_id in random.sample(device_ids, 100):  # simulate 100 devices at a time
            data = generate_sensor_data(device_id)
            try:
                producer.produce(KAFKA_TOPIC, key=device_id, value=json.dumps(data), callback=delivery_report)
                producer.poll(0)  # Process delivery callbacks immediately
            except BufferError as e:
                logger.warning(f"Local producer queue is full ({len(producer)} messages awaiting delivery): {e}")
                producer.poll(1)  # Wait up to 1 sec and retry

        producer.flush()
        time.sleep(FETCH_INTERVAL)  # send every second


if __name__ == "__main__":
    run_producer()
