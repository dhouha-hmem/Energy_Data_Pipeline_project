import json
from typing import List
# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType
# Project utilities
from src.utils.api_client import EnergyChartsClient
from src.utils.paths import bronze_table_path

"""
Bronze ingestion (Delta):
- Fetch raw public power production data from the Energy-Charts API
- Store the response exactly as received (raw JSON)
- Add minimal metadata (date, country, ingestion timestamp)
- Save the data into a Delta table 
"""


def ingest_public_power_de(
    spark: SparkSession,
    client: EnergyChartsClient,
    dates: List[str],
    country: str = "de",
    bronze_table: str = "public_power_de",

):
    # Collect all API responses before creating a Spark DataFrame
    all_records = []
    # Loop over each requested date
    for d in dates:
        payload = client.get_public_power(
            country=country,
            start_date=d,
            end_date=d,
        )
        # Store raw API response as JSON string
        all_records.append(
            {
                "country": country,
                "date": d,
                # keep raw payload as string
                "payload_json": json.dumps(payload, ensure_ascii=False),
            }
        )

    if not all_records:
        print("No public power data to ingest.")
        return

    # Explicit schema for the bronze table
    schema = StructType(
        [
            StructField("country", StringType(), False),
            StructField("date", StringType(), False),
            StructField("payload_json", StringType(), True),
        ]
    )

    # Create Spark DataFrame from collected API responses
    df = spark.createDataFrame(all_records, schema=schema)

    # Add ingestion metadata columns
    df = (
        # when data was ingested
        df.withColumn("ingested_at", current_timestamp())  
        # data source identifier    
          .withColumn("source", lit("energy-charts"))          
    )

    path = bronze_table_path(bronze_table)

    # Write data to Delta (overwrite for simplicity in local setup)
    (
        df.write
          .format("delta")
          .mode("overwrite")   
          .save(path)
    )

    print(f"✅ Bronze public power (DELTA) ingested for {len(dates)} day(s)")


