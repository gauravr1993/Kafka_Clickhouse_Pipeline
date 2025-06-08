import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
INFLUX_API_URL = os.getenv("INFLUX_API_URL")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", 5))

CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = ''
CLICKHOUSE_DB = 'iot'

ANOMALY_CHECK_INTERVAL = 10  # seconds for detector scheduler
