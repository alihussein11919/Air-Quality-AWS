import os
import json
import base64
import boto3

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
sns = boto3.client("sns")

def lambda_handler(event, context):
    for record in event.get("Records", []):
        raw_data = record["kinesis"]["data"]
        if isinstance(raw_data, str):
            raw_data = base64.b64decode(raw_data)
        payload = json.loads(raw_data)
        param = payload.get("parameter", "")
        value = payload.get("value", 0)
        location = payload.get("location", "")
        if param in ["pm25", "pm10"] and value > 150:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"Hazardous AQI Alert: {location}",
                Message=f"Hazardous {param} reading of {value} detected in {location}"
            )
    return {"statusCode": 200}

