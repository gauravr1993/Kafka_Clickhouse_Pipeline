import requests


def fetch_sensor_data(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[ERROR] API fetch failed: {e}")
        return []
