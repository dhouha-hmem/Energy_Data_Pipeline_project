from pyspark.sql import SparkSession
# Spark functions for aggregation + cleaning strings + date handling
from pyspark.sql.functions import col, avg, to_date, lower, trim, sum 
# Paths where silver and gold Delta tables are stored
from src.utils.paths import silver_table_path, gold_table_path


def build_gold_power_daily(
    spark: SparkSession,
    silver_table: str = "public_power_de_silver",
    gold_table: str = "power_daily_by_type",
):
    """
    Gold table (Delta) - Power daily by type:
    - Daily net electricity production by production type (Germany)
    - Input: Silver power table (one row per production value)
    - Output: Daily total production per production_type and date
    - We group by (date, production_type) and SUM the values
    """
    # Read silver Delta table
    df = spark.read.format("delta").load(silver_table_path(silver_table))

    # Aggregate to daily totals per production type
    #daily = (
    #    df.groupBy("date", "production_type")
    #      .agg(sum("value").alias("daily_net_production"))
    #)

    daily = (
        df.withColumn("date", to_date(col("timestamp")))   # add date
          .groupBy("date", "production_type")
          .agg(sum("value").alias("daily_net_production"))
    )


    # Write gold Delta table
    (daily.write
        .format("delta")
        .mode("overwrite")
        .save(gold_table_path(gold_table))
    )

    print("✅ Gold daily power production created (DELTA)")


def build_gold_price_daily(
    spark: SparkSession,
    silver_table: str = "price_de_lu_silver",
    gold_table: str = "price_daily",
):
    """
    Gold table (Delta) - Price daily average:
    - Input: Silver price table (one row per timestamp price)
    - Output: Daily average price (avg_price_eur_mwh)
    - We create a date column from timestamp then AVG the prices
    
    """
    # Read silver Delta table
    df = spark.read.format("delta").load(silver_table_path(silver_table))
    # Add "date" from timestamp, then aggregate daily average
    daily = (
        df.withColumn("date", to_date(col("timestamp")))   # add date
          .groupBy("date")
          .agg(avg("price_eur_mwh").alias("avg_price_eur_mwh"))
    )

    # Write gold Delta table
    (
        daily.write
            .format("delta")
            .mode("overwrite")
            .save(gold_table_path(gold_table))
    )

    print("✅ Gold daily price table created (DELTA)")


def build_gold_power_price_join(
    spark: SparkSession,
    power_gold_table: str = "power_daily_by_type",
    price_gold_table: str = "price_daily",
    gold_table: str = "power_price_daily",
):
    """
    Gold join table (Delta) - join power vs price:
    - Daily offshore wind production vs daily average price
    - Input: gold power_daily_by_type + gold price_daily
    - Output: one table that compares offshore wind daily production
      with daily average price (same date)
    """

    # Read both gold tables (Delta)
    power = spark.read.format("delta").load(gold_table_path(power_gold_table))
    price = spark.read.format("delta").load(gold_table_path(price_gold_table))
    # Ensure date columns are proper DATE type on both sides
    power = power.withColumn("date", to_date(col("date")))
    price = price.withColumn("date", to_date(col("date")))


    #print("Distinct production_type :")
    #power.select("production_type").distinct().orderBy("production_type").show(200, truncate=False)

    #offshore = power.filter(col("production_type") == "Wind offshore")
    # Filter only offshore wind rows (case + spaces normalized)
    offshore = power.filter(lower(trim(col("production_type"))) == "wind offshore")

    # Join offshore production with daily price on the same date
    joined = (
        offshore.join(price, on="date", how="inner")
                .select(
                    col("date"),
                    col("daily_net_production").alias("offshore_wind_daily"),
                    col("avg_price_eur_mwh"),
                )
    )
    # Write final gold join table
    (joined.write
        .format("delta")
        .mode("overwrite")
        .save(gold_table_path(gold_table))
    )

    print("✅ Gold power–price join table created (DELTA)")

