# detector/clickhouse_client.py
from clickhouse_driver import Client
from config import *
from datetime import datetime

client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB
)


def fetch_latest_data(last_ts):
    query = f"""
    SELECT device_id, timestamp, temperature
    FROM temperature_data
    WHERE timestamp > toDateTime('{last_ts}')
    ORDER BY timestamp
    """
    return client.execute(query)


def insert_anomalies(anomalies):
    if not anomalies:
        return
    # anomalies is list of dicts: [{device_id, timestamp, temperature, anomaly_type}, ...]
    insert_query = """
    INSERT INTO anomalies (device_id, timestamp, temperature, anomaly_type) VALUES
    """
    values = [
        (a['device_id'], datetime.fromisoformat(a['timestamp']), a['temperature'], a['anomaly_type'])
        for a in anomalies
    ]
    client.execute(
        insert_query,
        values
    )
