import requests
from datetime import datetime, timedelta
def get_yesterday_date():
    yesterday = datetime.utcnow().date() - timedelta(days=1)
    return yesterday.isoformat()


def fetch_weather_for_city(city_config):
    date_str = get_yesterday_date()

    base_url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": city_config["lat"],
        "longitude": city_config["lon"],
        "start_date": date_str,
        "end_date": date_str,
        "hourly": [
            "temperature_2m",
            "apparent_temperature",
            "relativehumidity_2m",
            "precipitation",
            "windspeed_10m",
            "surface_pressure",
        ],
    }

    response = requests.get(base_url, params=params, timeout=30)

    response.raise_for_status()

    return response.json()