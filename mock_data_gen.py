import boto3
import json
import random
import time
from datetime import datetime, timedelta

kinesis_client = boto3.client("kinesis", region_name="us-east-1")

ad_impressions_stream_name = "AdImpressionsStreamInput"
ad_clicks_stream_name = "ClicksStreamInput"

campaigns = [
    {"id": "cmp001", "name": "Black Friday Sale"},
    {"id": "cmp002", "name": "Holiday Travel Deals"},
    {"id": "cmp003", "name": "Tech Gadget Launch"},
    {"id": "cmp004", "name": "Back to School Essentials"},
    {"id": "cmp005", "name": "Fitness Gear Discount"},
]

publishers = [
    {"id": "pub001", "name": "TechCrunch"},
    {"id": "pub002", "name": "NY Times"},
    {"id": "pub003", "name": "BuzzFeed"},
    {"id": "pub004", "name": "AdWeek"},
    {"id": "pub005", "name": "Mashable"},
]

geo_locations = ["US", "IN", "DE", "JP", "UK", "FR", "CA", "AU"]
platforms = ["mobile", "desktop", "tablet"]
device_types = ["smartphone", "laptop", "tablet", "smartwatch"]

def generate_impression():
    """Generate a realistic ad impression event."""
    campaign = random.choice(campaigns)
    publisher = random.choice(publishers)
    current_time = datetime.utcnow()

    return {
        "ad_id": f"ad-{random.randint(1000, 9999)}",
        "impression_id": f"imp-{random.randint(100000, 999999)}",
        "campaign_id": campaign["id"],
        "campaign_name": campaign["name"],
        "publisher_id": publisher["id"],
        "publisher_name": publisher["name"],
        "event_time": current_time.isoformat(),
        "device_type": random.choice(device_types),
        "geo_location": random.choice(geo_locations),
        "platform": random.choice(platforms),
        "ad_type": random.choice(["banner", "video", "native"]),
        "bid_price": round(random.uniform(0.5, 20.0), 2),  # Bid price in USD
    }

def generate_click(impression):
    """Generate a realistic ad click event based on an impression."""
    click_time = datetime.fromisoformat(impression["event_time"]) + timedelta(
        seconds=random.randint(0, 30)
    )

    return {
        "ad_id": impression["ad_id"],
        "impression_id": impression["impression_id"],
        "campaign_id": impression["campaign_id"],
        "campaign_name": impression["campaign_name"],
        "publisher_id": impression["publisher_id"],
        "publisher_name": impression["publisher_name"],
        "event_time": click_time.isoformat(),
        "device_type": impression["device_type"],
        "geo_location": impression["geo_location"],
        "platform": impression["platform"],
        "click_price": round(random.uniform(0.5, 10.0), 2),  # Click price in USD
    }

def publish_to_kinesis(stream_name, event):
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(event),
        PartitionKey=event["ad_id"]
    )
    print(f"Published to {stream_name}: {event}")

if __name__ == "__main__":
    while True:
        impression = generate_impression()
        publish_to_kinesis(ad_impressions_stream_name, impression)

        if random.random() > 0.4:  # 60% chance of generating a click
            click = generate_click(impression)
            publish_to_kinesis(ad_clicks_stream_name, click)

        time.sleep(2)