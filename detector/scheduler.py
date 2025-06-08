# detector/scheduler.py
import time
from detector.detect_anomalies import detect
from config import ANOMALY_CHECK_INTERVAL


def run_scheduler():
    print("[INFO] Starting anomaly detection scheduler...")
    while True:
        detect()
        time.sleep(ANOMALY_CHECK_INTERVAL)


if __name__ == "__main__":
    run_scheduler()
