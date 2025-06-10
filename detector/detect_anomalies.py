# detector/detect_anomalies.py
from detector.clickhouse_client import fetch_latest_data, insert_anomalies
from detector.rules import temperature_threshold, sudden_jump, flat_line
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import os


CHECKPOINT_FILE = "last_processed.txt"

def load_last_timestamp():
    if not os.path.exists(CHECKPOINT_FILE):
        # Initialize with a default old timestamp if first run
        return (datetime.now(timezone.utc) - timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
    with open(CHECKPOINT_FILE, "r") as f:
        return datetime.fromisoformat(f.read().strip())

def save_last_timestamp(ts: datetime):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(ts.isoformat())



def detect():
    print("[INFO] Fetching latest sensor data...")

    last_ts = load_last_timestamp()
    print(f"[INFO] Fetching sensor data since {last_ts}")

    data = fetch_latest_data(last_ts)  # stored data from ClickHouse
    if not data:
        print("[INFO] No new data to process.")
        return
    # Convert to dict list
    records = [
        {'device_id': d[0], 'timestamp': d[1].isoformat(), 'temperature': d[2]}
        for d in data
    ]
    anomalies = []
    # Group by device_id to detect sudden jump and flat line
    grouped = defaultdict(list)
    for r in sorted(records, key=lambda x: (x['device_id'], x['timestamp'])):
        grouped[r['device_id']].append(r)

    for device_id, points in grouped.items():
        prev_point = None
        for i, point in enumerate(points):
            # Rule 1: Temperature threshold
            if temperature_threshold(point):
                anomalies.append({**point, 'anomaly_type': 'High Temperature'})
            # Rule 2: Sudden jump
            if prev_point and sudden_jump(prev_point, point):
                anomalies.append({**point, 'anomaly_type': 'Sudden Jump'})
            prev_point = point
        # Rule 3: Flatline detection on last 5 min
        # Filter last 5 minutes of data for this device
        current_time_iso = datetime.utcnow().isoformat()
        last_5_min_points = [p for p in points if datetime.fromisoformat(p['timestamp']) >= datetime.utcnow() - timedelta(minutes=5)]
        if flat_line(last_5_min_points, current_time_iso):
            anomalies.append({**points[-1], 'anomaly_type': 'Flatline'})

    print(f"[INFO] Found {len(anomalies)} anomalies")
    if anomalies:
        insert_anomalies(anomalies)

    # Update checkpoint to the latest timestamp from current batch
    latest_ts = max(datetime.fromisoformat(r['timestamp']) for r in records)
    save_last_timestamp(latest_ts)

if __name__ == "__main__":
    detect()
