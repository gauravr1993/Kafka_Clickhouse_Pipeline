# sensors.py
import random
from datetime import datetime, timezone


def generate_sensor_data(device_id):
    return {
        'device_id': device_id,
        'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        'temperature': round(random.uniform(15, 85), 2)
    }