# detector/rules.py
from datetime import datetime, timedelta


def temperature_threshold(data_point):
    return data_point['temperature'] > 75


def sudden_jump(prev_point, curr_point):
    if not prev_point:
        return False
    time_diff = (datetime.fromisoformat(curr_point['timestamp']) - datetime.fromisoformat(prev_point['timestamp'])).total_seconds()
    temp_diff = abs(curr_point['temperature'] - prev_point['temperature'])
    return time_diff <= 5 and temp_diff > 15


def flat_line(data_points, current_time_iso):
    # data_points: list of points for the same device in recent period (sorted ascending by timestamp)
    # Checks if temperature did NOT change for 5+ minutes
    if len(data_points) < 2:
        return False
    start_time = datetime.fromisoformat(data_points[0]['timestamp'])
    end_time = datetime.fromisoformat(data_points[-1]['timestamp'])
    current_time = datetime.fromisoformat(current_time_iso)
    # If latest data is older than 5 min, flatline considered
    if (current_time - end_time).total_seconds() > 300:
        return True
    # Check if temp is constant throughout
    temps = [p['temperature'] for p in data_points]
    return len(set(temps)) == 1
