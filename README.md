
**Ad Campaign Real-Time Data Processing Architecture**
<img width="592" alt="image" src="https://github.com/user-attachments/assets/c0a365e5-4ddc-4535-8f41-8d9ce5038527" />


**Overview**

This architecture represents a complete real-time data processing pipeline for ad tech analytics that integrates several AWS services and open-source technologies. The pipeline captures ad impressions and clicks data streams, processes them with Apache Flink for event correlation, and then applies further transformations using AWS Glue before storing the enriched data in Apache Iceberg tables for analysis via Amazon Athena.


**Architecture Components:**


1. Data Generation and Ingestion

Mock Data Generator (mock_data_gen.py): Generates synthetic ad impression and click events, simulating a real ad tech platform with campaign data, publisher information, and user interaction metrics.
Kinesis Data Streams:

AdImpressionsStreamInput: Captures all ad impression events
ClicksStreamInput: Captures all ad click events
AdResultantOutput: Destination for joined/processed data


Stream Creation Utility (create_kinesis_streams.py): Sets up the required Kinesis streams with the proper configurations

2. Stream Processing with Apache Flink

Main Flink Application (main.py):

Defines streaming tables for impressions and clicks
Performs time-bounded join operations (matching clicks to impressions within a 30-second window)
Processes data in real-time and outputs results to the output Kinesis stream


Dependencies Management:

pom.xml: Maven configuration file that manages the Java dependencies
assembly.xml: Assembly configuration to package the application
Packaging creates a JAR file with all dependencies (pyflink-dependencies.jar)



3. Post-Processing with AWS Glue

Glue ETL Job (glue_post_processing_streaming_job.py):

Reads from the output Kinesis stream
Performs data validation and cleansing
Applies business rules and additional transformations
Calculates derived metrics (engagement duration, platform categorization, etc.)
Uses Apache Spark structured streaming



4. Data Storage and Access

Apache Iceberg Tables:

Stored in S3 with partitioning by campaign_id
Tables managed by AWS Glue Catalog
Support for ACID transactions and time travel queries


Amazon Athena: Provides SQL query interface to analyze the processed data

5. Configuration Management

Application Properties (application_properties.json):

Centralized configuration for all components
Defines stream names, AWS regions, and runtime options



**Data Flow:**

The mock data generator creates ad impression events and sends them to the AdImpressionsStreamInput Kinesis stream
For some impressions (60% chance), it also generates corresponding click events sent to the ClicksStreamInput stream
The Flink application consumes from both streams and joins impressions with their corresponding clicks based on matching ad_id and a time window
The joined results are written to the AdResultantOutput Kinesis stream
The Glue ETL job continuously reads from the output stream and:

**Validates the data**
Enriches it with additional business metrics
Writes to Apache Iceberg tables in S3 using merge operations


Data analysts can query the enriched data using SQL through Amazon Athena

**File Descriptions:**

application_properties.json

Central configuration file containing stream names, regions, and runtime settings
Used by both Flink and the stream creation utility


create_kinesis_streams.py

Utility script that reads the configuration and creates the required Kinesis streams
Sets up three streams: impressions input, clicks input, and joined output


main.py

Core Flink PyFlink application code
Defines table sources and sinks using SQL DDL
Implements the time-bounded join logic for correlating impressions and clicks


mock_data_gen.py

Generates synthetic ad tech data
Creates realistic impression events with campaign info, geo data, device types
Simulates clicks with proper correlation to impressions


glue_post_processing_streaming_job.py

AWS Glue job for downstream processing
Adds business logic and transformations
Handles data validation and enrichment
Manages the write to Iceberg tables with merge capabilities


pom.xml

Maven project configuration
Defines dependencies for the Flink application
Configures the build process for the JAR file


assembly.xml

Defines how the application should be packaged
Creates a ZIP file containing the Python code and JAR dependencies



**Key Technical Features:**

Time-bounded joins in Flink to correlate impressions and clicks within a 30-second window
Watermark handling for managing event time processing and late data
Stream-to-table merges using Iceberg's ACID transaction support
Business metrics calculation (engagement duration, revenue calculations)
Data validation and cleansing in the Glue processing pipeline
Partitioning by campaign_id for optimized query performance




******# Implementation Guide for Ad Tech Real-Time Streaming Platform******

## Step 1: Project Setup and Package Creation
- Set up the project structure with all required files
- Run `mvn clean install` in the project directory to generate the deployment package
- Verify the creation of `ad-flink-streaming-1.0.0.zip` in the target directory

## Step 2: Create Kinesis Streams
- Configure AWS credentials using `aws configure`
- Run the `create_kinesis_streams.py` script to create the three required Kinesis streams:
  - AdImpressionsStreamInput
  - ClicksStreamInput
  - AdResultantOutput
- Verify streams are active in the AWS Console

## Step 3: Set Up Apache Flink Application
- Create a new Managed Service for Apache Flink application through the AWS Console
- Upload the `.zip` file generated by Maven
- Configure application properties using the groups defined in `application_properties.json`
- Start the Flink application and verify it's running through the dashboard
<img width="953" alt="Screenshot 2025-05-18 024202" src="https://github.com/user-attachments/assets/8ed50045-7492-4c3d-80ad-73426b259f28" />

## Step 4: Set Up AWS Glue Streaming ETL Job
- Create an S3 bucket for Glue assets and Iceberg data
- Upload the Glue script (`glue_post_processing_streaming_job.py`) to the S3 bucket
- Create a Glue streaming job through the AWS Console, pointing to the S3 script
- Configure job parameters and start the Glue job


## Step 5: Generate Test Data and Monitor
- Run the mock data generator (`mock_data_gen.py`) to produce test events
- Monitor each component of the pipeline:
  - Kinesis stream metrics
  - Flink application dashboard
    <img width="795" alt="Screenshot 2025-05-18 035339" src="https://github.com/user-attachments/assets/86a435c7-89bd-4cc8-ae88-c3cbbf6d74a7" />

    
  - S3 Iceberg data files
  <img width="948" alt="Screenshot 2025-05-18 035603" src="https://github.com/user-attachments/assets/42db2633-fbee-4bfb-8b44-ed30c105a0d9" />

  - Glue job execution logs
  <img width="782" alt="Screenshot 2025-05-18 035315" src="https://github.com/user-attachments/assets/b7ff4e1d-caff-41bf-b255-44f25db0ba00" />

## Step 6: Query Data with Amazon Athena
- Set up Athena workgroup with an S3 location for query results
- Run queries against the Iceberg table in the Glue catalog
<img width="955" alt="Screenshot 2025-05-18 040334" src="https://github.com/user-attachments/assets/219407d1-146d-4b09-ac53-47ae92f57795" />



