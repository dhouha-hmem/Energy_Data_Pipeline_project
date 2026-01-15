from pyspark.sql import SparkSession
# Spark functions used for JSON parsing + array flattening
from pyspark.sql.functions import ( col, explode, to_timestamp, from_json, get_json_object,
    arrays_zip, explode, from_unixtime, coalesce )
# Spark data types used to define JSON schemas
from pyspark.sql.types import ( StructType, StructField, StringType,
    DoubleType, ArrayType, LongType
)
# Project paths (where bronze/silver Delta tables are stored)
from src.utils.paths import bronze_table_path, silver_table_path

# Bronze stores raw JSON as a STRING column called payload_json.
# In Silver we parse this JSON string into a structured Spark object.

# public_power payload_json example (Energy-Charts):
# {
#   "unix_seconds": [...],
#   "production_types": [
#       {"name":"Wind offshore","data":[...]},
#       {"name":"Wind onshore","data":[...]}
#   ],...}

power_payload_schema = StructType([
    StructField("unix_seconds", ArrayType(DoubleType()), True),
    StructField("production_types", ArrayType(
        StructType([
            StructField("name", StringType(), True),
            StructField("data", ArrayType(DoubleType()), True),
        ])
    ), True),
    StructField("deprecated", StringType(), True),
])

# price payload_json (Energy-Charts) usually looks like:
# {
#   "unix_seconds": [...],
#   "price": [...],
#   "unit": "EUR / MWh"
# }
price_payload_schema = StructType([
    StructField("data", ArrayType(
        StructType([
            StructField("timestamp", StringType(), True),
            StructField("value", DoubleType(), True),
        ])
    ), True)
])

# -------------------- Bronze -> Silver: Public Power --------------------
def bronze_power_to_silver(
    spark: SparkSession,
    bronze_table: str = "public_power_de",
    silver_table: str = "public_power_de_silver",
):
    # Read bronze as DELTA table
    bronze_df = spark.read.format("delta").load(bronze_table_path(bronze_table))

    # Parse JSON string into a struct column (payload)
    df = bronze_df.withColumn(
        "payload", from_json(col("payload_json"), power_payload_schema)
    )

    # Explode production_types -> one row per production type per day
    # Then zip unix_seconds with the corresponding values -> keep timestamp alignment
    exploded = (
        df.select(
            col("country"),
            col("date"),
            col("payload.unix_seconds").cast(ArrayType(LongType())).alias("unix_seconds"),
            explode(col("payload.production_types")).alias("pt"),
        )
        .select(
            col("country"),
            col("date"),
            col("unix_seconds"),
            col("pt.name").alias("production_type"),
            col("pt.data").alias("values"),
        )
        # Zip timestamps with values: [{unix_seconds:..., values:...}, ...]
        .withColumn("pairs", arrays_zip(col("unix_seconds"), col("values")))
        .select(
            col("country"),
            col("date"),
            col("production_type"),
            explode(col("pairs")).alias("p"),
        )
        .select(
            col("country"),
            col("date"),
            col("production_type"),
            to_timestamp(from_unixtime(col("p.unix_seconds"))).alias("timestamp"),
            col("p.values").cast("double").alias("value"),
        )
        # Drop bad rows
        .where(col("timestamp").isNotNull() & col("value").isNotNull())
    )

    # Write silver Delta table
    (
        exploded.write
        .format("delta")
        .mode("overwrite")
        .save(silver_table_path(silver_table))
    )

    print("✅ Silver public power table created (DELTA)")




# -------------------- Bronze -> Silver: Prices --------------------
def bronze_price_to_silver(
    spark: SparkSession,
    bronze_table: str = "price_de_lu",
    silver_table: str = "price_de_lu_silver",
):
    # Read bronze Delta table
    bronze_df = spark.read.format("delta").load(bronze_table_path(bronze_table))
    # Extract unix timestamps array from JSON string
    unix_arr = from_json(get_json_object(col("payload_json"), "$.unix_seconds"), ArrayType(LongType()))

    #Extract prices array from JSON string
    # Try common field names for prices (can add more if needed)
    price_arr_1 = from_json(get_json_object(col("payload_json"), "$.price"), ArrayType(DoubleType()))
    price_arr_2 = from_json(get_json_object(col("payload_json"), "$.prices"), ArrayType(DoubleType()))
    price_arr_3 = from_json(get_json_object(col("payload_json"), "$.data"), ArrayType(DoubleType()))

    # Build a clean DF with market + arrays
    df = bronze_df.select(
        col("market"),
        unix_arr.alias("unix_seconds"),
        coalesce(price_arr_1, price_arr_2, price_arr_3).alias("prices")
    )

    # Zip the two arrays together so each timestamp matches the correct price
    #  arrays_zip creates an array of structs: [{unix_seconds:..., prices:...}, ...]
    exploded = (
        df
        .withColumn("pairs", arrays_zip(col("unix_seconds"), col("prices")))
        .select("market", explode(col("pairs")).alias("p"))
        .select(
            col("market"),
            to_timestamp(from_unixtime(col("p.unix_seconds"))).alias("timestamp"),
            col("p.prices").cast("double").alias("price_eur_mwh")
        )
        # Drop bad rows (null timestamp or null price)
        .where(col("timestamp").isNotNull() & col("price_eur_mwh").isNotNull())
    )

    
    # Write silver Delta table
    (
        exploded.write
        .format("delta")
        .mode("overwrite")
        .save(silver_table_path(silver_table))
    )

    print("✅ Silver price table created (DELTA)")
    

