import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, when, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Real-Time-Ad-Tech-Data-Processing")

# Global Spark Session
g_spark = (
    SparkSession.builder.appName("Real-Time-Ad-Tech-Data-Processing")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://iceberg-adcampaign/warehouse/")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .getOrCreate()
)

kinesis_stream = "AdResultantOutput"
aws_region = "us-east-1"

# Define schema for incoming data
schema = StructType([
    StructField("ad_id", StringType(), True),
    StructField("impression_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("publisher_id", StringType(), True),
    StructField("impression_time", TimestampType(), True),
    StructField("click_time", TimestampType(), True),
    StructField("geo_location", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("bid_price", DoubleType(), True),
    StructField("click_price", DoubleType(), True)
])

# Read from Kinesis stream from the start
logger.info(f"Reading data from Kinesis stream: {kinesis_stream}")
raw_stream = (
    g_spark.readStream
    .format("kinesis")
    .option("streamName", kinesis_stream)
    .option("region", aws_region)
    .option("startingposition", "LATEST")
    .load()
)

# Parse JSON and apply schema
parsed_stream = raw_stream.selectExpr("CAST(data AS STRING)").select(
    from_json(col("data"), schema).alias("data")
).select("data.*")

validated_stream = parsed_stream.filter(
    (col("bid_price").isNotNull() & (col("bid_price") > 0)) &
    (col("click_price").isNotNull() & (col("click_price") > 0)) &
    col("ad_id").isNotNull() &
    col("impression_id").isNotNull()
)

# Business Rules and Transformations
transformed_stream = (
    validated_stream
    .withColumn("is_premium_ad", when(col("bid_price") > 50, lit("Yes")).otherwise(lit("No")))
    .withColumn("engagement_duration", (col("click_time").cast("long") - col("impression_time").cast("long")))
    .withColumn("platform_category", when(col("platform").isin("mobile", "smartphone"), lit("Mobile"))
                .when(col("platform").isin("tablet"), lit("Tablet"))
                .otherwise(lit("Desktop")))
    .withColumn("click_revenue", col("click_price") * lit(1.2))
    .withColumn("processed_time", expr("current_timestamp()"))
)

# Ensure Iceberg table exists
database_name = "ad_campgain"
table_name = "enriched_ad_data"
logger.info(f"Ensuring Iceberg table glue_catalog.{database_name}.{table_name} exists...")
g_spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database_name}")
g_spark.sql(f"""
CREATE TABLE IF NOT EXISTS glue_catalog.{database_name}.{table_name} (
    ad_id STRING,
    impression_id STRING,
    campaign_id STRING,
    publisher_id STRING,
    impression_time TIMESTAMP,
    click_time TIMESTAMP,
    geo_location STRING,
    platform STRING,
    device_type STRING,
    bid_price DOUBLE,
    click_price DOUBLE,
    is_premium_ad STRING,
    engagement_duration LONG,
    platform_category STRING,
    click_revenue DOUBLE,
    processed_time TIMESTAMP
)
PARTITIONED BY (campaign_id)
TBLPROPERTIES ('write.format.default'='parquet', 'write.target-file-size-bytes'='536870912')
""")

# Logging function for mini-batches
def log_mini_batch(batch_df, batch_id):
    logger.info(f"Processing batch {batch_id}")
    logger.info(f"Batch Schema: {batch_df.printSchema()}")
    logger.info(f"Batch Data (showing first 5 rows): {batch_df.show(5, truncate=False)}")

# Define upsert (merge query) using global temporary views
def merge_to_iceberg(spark_session, batch_df, batch_id):
    try:
        logger.info(f"Processing batch {batch_id} for Iceberg merge...")

        if batch_df.isEmpty():
            logger.warning(f"Batch {batch_id} is empty. Skipping merge.")
            return

        logger.info(f"Batch Schema: {batch_df.printSchema()}")
        logger.info(f"Batch Data (showing first 5 rows): {batch_df.show(5, truncate=False)}")

        # Global temporary view name
        view_name = f"streaming_batch_{batch_id}"
        global_view_name = f"global_temp.{view_name}"

        # Uncache any existing table with the same name
        spark_session.sql(f"UNCACHE TABLE IF EXISTS {global_view_name}")

        # Create or replace the global temporary view
        batch_df.createOrReplaceGlobalTempView(view_name)

        # Cache the global temporary view for faster access
        spark_session.sql(f"CACHE TABLE {global_view_name}")

        # Merge query
        merge_query = f"""
        MERGE INTO glue_catalog.{database_name}.{table_name} tgt
        USING {global_view_name} src
        ON tgt.ad_id = src.ad_id AND tgt.impression_id = src.impression_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        spark_session.sql(merge_query)

        logger.info(f"Batch {batch_id} successfully merged into Iceberg table.")

    except Exception as e:
        logger.error(f"Error during merge for batch {batch_id}: {e}")
        raise e

# Write the stream to Iceberg with logging and session isolation
def start_streaming_query():
    query = (
        transformed_stream.writeStream
        .foreachBatch(lambda df, id: merge_to_iceberg(g_spark.newSession(), df, id))  # Use isolated session
        .option("checkpointLocation", "s3://iceberg-adcampaign/checkpoints/")
        .start()
    )
    query.awaitTermination()

start_streaming_query()