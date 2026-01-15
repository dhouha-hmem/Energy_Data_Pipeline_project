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
    - Fetch raw electricity price data from the Energy-Charts API
    - Store the API response exactly as received (raw JSON)
    - Add minimal metadata (market, date, ingestion timestamp)
    - Write the result into a Delta table (bronze layer)
"""

def ingest_price_de_lu(
    spark: SparkSession,
    client: EnergyChartsClient,
    dates: List[str],
    market: str = "DE-LU",
    bronze_table: str = "price_de_lu",
):
    # Collect all API responses before creating Spark DataFrame
    all_records = []

    # Loop over each date (daily ingestion or backfill)
    for d in dates:
        # Call Energy-Charts price API for one day
        payload = client.get_price(
            market=market, 
            start_date=d, 
            end_date=d)

        # Store raw JSON response
        all_records.append(
            {
                "market": market,
                "date": d,
                "payload_json": json.dumps(payload, ensure_ascii=False),
            }
        )

    if not all_records:
        print("No price data to ingest.")
        return

    # Explicit schema for the bronze price table
    schema = StructType(
        [
            StructField("market", StringType(), False),
            StructField("date", StringType(), False),
            StructField("payload_json", StringType(), True),
        ]
    )

    # Create Spark DataFrame from collected API responses
    df = spark.createDataFrame(all_records, schema=schema)
    # Add ingestion metadata
    df = (
        # ingestion timestamp
        df.withColumn("ingested_at", current_timestamp())    
        # data source  
          .withColumn("source", lit("energy-charts"))          
    )

    # Resolve bronze Delta table path
    path = bronze_table_path(bronze_table)

    # Write to Delta (overwrite for local execution simplicity)
    (
        df.write
          .format("delta")
          .mode("overwrite")   
          .save(path)
    )


    print(f"✅ Bronze price (DELTA) ingested for {len(dates)} day(s)")
