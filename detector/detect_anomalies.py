# detector/detect_anomalies.py
from detector.clickhouse_client import fetch_latest_data, insert_anomalies
from detector.rules import temperature_threshold, sudden_jump, flat_line
from datetime import datetime, timedelta
from collections import defaultdict


def detect():
    print("[INFO] Fetching latest sensor data...")
    data = fetch_latest_data(1000)  # list of tuples from ClickHouse
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
    insert_anomalies(anomalies)


if __name__ == "__main__":
    detect()
