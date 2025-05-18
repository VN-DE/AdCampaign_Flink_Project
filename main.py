import os
import json
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AdTechStreamingPipeline")

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"

is_local = (
    True if os.environ.get("IS_LOCAL") else False
)

if is_local:

    APPLICATION_PROPERTIES_FILE_PATH = "application_properties.json"
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    print(f"{CURRENT_DIR}")
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        f"file:///{CURRENT_DIR}/target/pyflink-dependencies.jar"
    )

    # table_env.get_config().get_configuration().set_string(
    #     "pipeline.jars",
    #     "file:///C:/Users/ASUS/Downloads/pyflink-dependencies.jar"
    # )


def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            return json.load(file)
    else:
        logger.error(f"A file at {APPLICATION_PROPERTIES_FILE_PATH} was not found")
        return {}

def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]

def main():
    properties = get_application_properties()
    ad_impressions_stream_prop = property_map(properties, "AdImpressionsStream")
    clicks_stream_prop = property_map(properties, "AdClicksStream")
    output_stream_prop = property_map(properties, "AdDestinationStream")

    # Define Ad Impressions source
    table_env.execute_sql(f"""
        CREATE TABLE ad_impressions (
            ad_id STRING,
            impression_id STRING,
            campaign_id STRING,
            publisher_id STRING,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
            device_type STRING,
            geo_location STRING,
            platform STRING,
            ad_type STRING,
            bid_price DOUBLE
        )
        WITH (
            'connector' = 'kinesis',
            'stream' = '{ad_impressions_stream_prop["stream.name"]}',
            'aws.region' = '{ad_impressions_stream_prop["aws.region"]}',
            'format' = 'json',
            'scan.stream.initpos' = 'LATEST',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # Define Ad Clicks source
    table_env.execute_sql(f"""
        CREATE TABLE ad_clicks (
            ad_id STRING,
            impression_id STRING,
            campaign_id STRING,
            publisher_id STRING,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
            device_type STRING,
            geo_location STRING,
            platform STRING,
            click_price DOUBLE
        )
        WITH (
            'connector' = 'kinesis',
            'stream' = '{clicks_stream_prop["stream.name"]}',
            'aws.region' = '{clicks_stream_prop["aws.region"]}',
            'format' = 'json',
            'scan.stream.initpos' = 'LATEST',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # Define Joined Stream Sink
    table_env.execute_sql(f"""
        CREATE TABLE joined_output (
            ad_id STRING,
            impression_id STRING,
            campaign_id STRING,
            publisher_id STRING,
            impression_time TIMESTAMP(3),
            click_time TIMESTAMP(3),
            geo_location STRING,
            platform STRING,
            device_type STRING,
            bid_price DOUBLE,
            click_price DOUBLE
        )
        WITH (
            'connector' = 'kinesis',
            'stream' = '{output_stream_prop["stream.name"]}',
            'aws.region' = '{output_stream_prop["aws.region"]}',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # Uncomment this part to register table in flink
    # to print data on console
    # table_env.execute_sql("""
    #     CREATE TABLE joined_output (
    #             ad_id STRING,
    #             impression_id STRING,
    #             campaign_id STRING,
    #             publisher_id STRING,
    #             impression_time TIMESTAMP(3),
    #             click_time TIMESTAMP(3),
    #             geo_location STRING,
    #             platform STRING,
    #             device_type STRING,
    #             bid_price DOUBLE,
    #             click_price DOUBLE
    #           )
    #           WITH (
    #             'connector' = 'print'
    #           )""")

    # Perform the time-bounded join and write to output
    table_result = table_env.execute_sql("""
        INSERT INTO joined_output
        SELECT 
            i.ad_id,
            i.impression_id,
            i.campaign_id,
            i.publisher_id,
            i.event_time AS impression_time,
            CAST(c.event_time AS TIMESTAMP(3)) AS click_time,
            i.geo_location,
            i.platform,
            i.device_type,
            i.bid_price,
            c.click_price
        FROM ad_impressions i
        JOIN ad_clicks c
        ON i.ad_id = c.ad_id
        AND c.event_time BETWEEN i.event_time - INTERVAL '30' SECOND AND i.event_time + INTERVAL '30' SECOND
    """)

    logger.info("Flink job submitted successfully.")
    if is_local:
        table_result.wait()

if __name__ == "__main__":
    main()