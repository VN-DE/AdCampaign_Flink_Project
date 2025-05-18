import boto3
import time
import json

def create_kinesis_streams():
    """
    Creates the three Kinesis streams required for the ad tech streaming application:
    - AdImpressionsStreamInput
    - ClicksStreamInput
    - AdResultantOutput
    """
    # Read configuration from application properties
    with open("application_properties.json", "r") as file:
        properties = json.load(file)
    
    # Extract stream names and region from properties
    impressions_stream_name = None
    clicks_stream_name = None
    output_stream_name = None
    region = None
    
    for prop in properties:
        if prop["PropertyGroupId"] == "AdImpressionsStream":
            impressions_stream_name = prop["PropertyMap"]["stream.name"]
            region = prop["PropertyMap"]["aws.region"]
        elif prop["PropertyGroupId"] == "AdClicksStream":
            clicks_stream_name = prop["PropertyMap"]["stream.name"]
        elif prop["PropertyGroupId"] == "AdDestinationStream":
            output_stream_name = prop["PropertyMap"]["stream.name"]
    
    if not all([impressions_stream_name, clicks_stream_name, output_stream_name, region]):
        raise ValueError("Missing required stream configuration in application_properties.json")
    
    # Initialize Kinesis client
    kinesis_client = boto3.client("kinesis", region_name=region)
    
    # List of streams to create
    streams = [
        {"name": impressions_stream_name, "shard_count": 1},
        {"name": clicks_stream_name, "shard_count": 1},
        {"name": output_stream_name, "shard_count": 1}
    ]
    
    # Create each stream if it doesn't already exist
    existing_streams = kinesis_client.list_streams()["StreamNames"]
    
    for stream in streams:
        if stream["name"] in existing_streams:
            print(f"Stream {stream['name']} already exists. Skipping creation.")
            continue
        
        print(f"Creating Kinesis stream: {stream['name']}")
        kinesis_client.create_stream(
            StreamName=stream["name"],
            ShardCount=stream["shard_count"]
        )
    
    # Wait for all streams to become active
    for stream in streams:
        print(f"Waiting for stream {stream['name']} to become active...")
        stream_active = False
        
        while not stream_active:
            response = kinesis_client.describe_stream(StreamName=stream["name"])
            status = response["StreamDescription"]["StreamStatus"]
            
            if status == "ACTIVE":
                stream_active = True
                print(f"Stream {stream['name']} is now active")
            else:
                print(f"Stream {stream['name']} status: {status}. Waiting...")
                time.sleep(5)

if __name__ == "__main__":
    create_kinesis_streams()
    print("All Kinesis streams have been created successfully!")
