# sensors.py
import random
from datetime import datetime


def generate_sensor_data(device_id):
    return {
        'device_id': device_id,
        'timestamp': datetime.utcnow().replace(microsecond=0).isoformat(),
        'temperature': round(random.uniform(15, 85), 2)
    }