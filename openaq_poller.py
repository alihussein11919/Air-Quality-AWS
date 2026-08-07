import os
import json
import boto3
import urllib.request
import urllib.error
from datetime import datetime, timezone

KINESIS_STREAM = os.environ["KINESIS_STREAM_NAME"]
OPENAQ_API_KEY = os.environ["OPENAQ_API_KEY"]
OPENAQ_BASE_URL = "https://api.openaq.org/v3"

PARAMETERS = {
    2: "pm25",
    1: "pm10",
    5: "o3",
    8: "no2",
    3: "so2",
    6: "co",
}

kinesis = boto3.client("kinesis")


def lambda_handler(event, context):
    measurements = fetch_latest_measurements()
    if not measurements:
        return {"statusCode": 200, "body": "No new measurements"}

    pushed = push_to_kinesis(measurements)
    return {
        "statusCode": 200,
        "body": json.dumps({"fetched": len(measurements), "pushed": pushed}),
    }


def fetch_latest_measurements():
    measurements = []

    for param_id, param_name in PARAMETERS.items():
        url = f"{OPENAQ_BASE_URL}/parameters/{param_id}/latest?limit=50"
        req = urllib.request.Request(url)
        req.add_header("X-API-Key", OPENAQ_API_KEY)

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                for result in data.get("results", []):
                    coords = result.get("coordinates", {})
                    dt = result.get("datetime", {})
                    utc_time = dt.get("utc", "") if isinstance(dt, dict) else ""

                    measurements.append({
                        "location": f"loc_{result.get('locationsId', 'unknown')}",
                        "parameter": param_name,
                        "value": result.get("value"),
                        "unit": "ug/m3",
                        "timestamp": utc_time,
                        "latitude": coords.get("latitude"),
                        "longitude": coords.get("longitude"),
                        "country": "",
                        "city": "",
                        "source_name": "openaq",
                    })
        except urllib.error.URLError as e:
            print(f"OpenAQ API error for {param_name}: {e}")
            continue
        except Exception as e:
            print(f"Error for {param_name}: {e}")
            continue

    return measurements


def push_to_kinesis(measurements):
    records = []
    for m in measurements:
        if m.get("value") is None:
            continue
        records.append({
            "Data": (json.dumps({
                "location": m.get("location"),
                "parameter": m.get("parameter"),
                "value": m.get("value"),
                "unit": m.get("unit"),
                "timestamp": m.get("timestamp"),
                "latitude": m.get("latitude"),
                "longitude": m.get("longitude"),
                "country": m.get("country"),
                "city": m.get("city"),
                "source_name": m.get("source_name"),
            }) + "\n").encode("utf-8"),
            "PartitionKey": str(m.get("location", "default")),
        })

    pushed = 0
    for i in range(0, len(records), 500):
        batch = records[i : i + 500]
        resp = kinesis.put_records(
            StreamName=KINESIS_STREAM, Records=batch
        )
        pushed += resp["Records"].__len__() - len(
            [r for r in resp["Records"] if "ErrorCode" in r]
        )

    return pushed
