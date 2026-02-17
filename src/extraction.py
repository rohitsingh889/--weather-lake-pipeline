from config.cities import CITIES
from src.api_client import fetch_weather_for_city, get_yesterday_date
from src.s3_writer import write_raw_to_s3

def run_extraction():
    date_str = get_yesterday_date()
    bucket_name = "rohitbucket1weather"   # BUCKET NAME

    processed = []

    for city in CITIES:
        print(f"Fetching data for {city['city']}...")

        weather_json = fetch_weather_for_city(city)

        write_raw_to_s3(
            bucket=bucket_name,
            city=city["city"],
            date_str=date_str,
            weather_json=weather_json,
        )

        processed.append(city["city"])

    return processed