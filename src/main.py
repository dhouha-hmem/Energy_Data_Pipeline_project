import yaml
# Spark + API utilities
from src.utils.spark_session import get_spark_session
from src.utils.api_client import EnergyChartsClient
from src.utils.dates import get_ingestion_dates
# Bronze ingestion jobs
from src.ingestion.power_ingestion import ingest_public_power_de
from src.ingestion.price_ingestion import ingest_price_de_lu
# Transformations
from src.transformations.bronze_to_silver import (
    bronze_power_to_silver,
    bronze_price_to_silver,
)

from src.transformations.silver_to_gold import (
    build_gold_power_daily,
    build_gold_price_daily,
    build_gold_power_price_join,
)


# Load pipeline settings from YAML config file.
def load_config(path: str = "config/config.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    # 1-Load config (dates, spark config, API settings, etc.)
    config = load_config()

    # 2-Start Spark session
    spark = get_spark_session(
        app_name=config["spark"]["app_name"],
        master=config["spark"]["master"],
        log_level=config["spark"]["log_level"],
    )

    # 3-Create API client (Energy-Charts)
    client = EnergyChartsClient(
        base_url=config["source"]["base_url"],
        timeout_seconds=config["source"]["timeout_seconds"],
    )

    # 4-Decide which dates to ingest (backfill range from config.yaml)

    start_date = config["run"]["backfill"]["start_date"]
    end_date = config["run"]["backfill"]["end_date"]

    dates = get_ingestion_dates(
        start_date=start_date,
        end_date=end_date,
    )

    print(
        f"➡️ Ingesting data from {start_date} to {end_date} "
        f"({len(dates)} day(s))"
    )

    # 5-Bronze ingestion:  fetch raw API payloads and write to Delta
    ingest_public_power_de(
        spark=spark,
        client=client,
        dates=[d.isoformat() for d in dates],
    )

    ingest_price_de_lu(
        spark=spark,
        client=client,
        dates=[d.isoformat() for d in dates],
    )

    # 6- SILVER layer(Bronze → Silver): parse JSON payloads into structured tabular format
    bronze_power_to_silver(spark)
    bronze_price_to_silver(spark)

    # 7-GOLD layer(Silver → Gold): daily aggregations (business-ready tables)
    build_gold_power_daily(spark)
    build_gold_price_daily(spark)
    # 8-Gold join: : combine offshore wind production + daily average price
    build_gold_power_price_join(spark)

    # 9-DEBUG BLOCK 
    print("== DEBUG COUNTS ==")

    power_bronze = spark.read.format("delta").load("data/bronze/public_power_de")
    price_bronze = spark.read.format("delta").load("data/bronze/price_de_lu")
    print("power_bronze count =", power_bronze.count())
    print("price_bronze count =", price_bronze.count())

    power_silver = spark.read.format("delta").load("data/silver/public_power_de_silver")
    price_silver = spark.read.format("delta").load("data/silver/price_de_lu_silver")
    print("power_silver count =", power_silver.count())
    print("price_silver count =", price_silver.count())

    power_gold = spark.read.format("delta").load("data/gold/power_daily_by_type")
    price_gold = spark.read.format("delta").load("data/gold/price_daily")
    power_price_gold = spark.read.format("delta").load("data/gold/power_price_daily")
    print("power_gold count =", power_gold.count())
    print("price_gold count =", price_gold.count())
    print("power_price_gold count =", power_price_gold.count())

    print("power_gold sample:")
    power_gold.show(30, truncate=False)

    print("price_gold sample:")
    price_gold.show(10, truncate=False)

    print("power_price_gold sample:")
    power_price_gold.show(10, truncate=False)


    print("✅ Pipeline finished successfully")
    spark.stop()

# Run pipeline only when this file is executed directly
if __name__ == "__main__":
    main()

