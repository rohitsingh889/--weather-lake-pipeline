import boto3
import json

s3 = boto3.client("s3")

def build_s3_key(city, date_str):
    year, month, day = date_str.split("-")

    return (
        f"bronze/weather/"
        f"city={city}/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"weather.json"
    )

def write_raw_to_s3(bucket, city, date_str, weather_json):
    key = build_s3_key(city, date_str)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(weather_json),
        ContentType="application/json",
    )

    print(f"Uploaded → {key}")