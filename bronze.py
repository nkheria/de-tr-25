from dlt import table
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
# :white_check_mark: Define schema for retail IoT CSV
iot_schema = StructType([
    StructField("OrderID", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("StoreID", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("SalesAmount", DoubleType(), True),
    StructField("OrderTime", StringType(), True),  # can cast to Timestamp later in Silver
    StructField("DeviceType", StringType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("DeviceStatus", StringType(), True),
    StructField("AnomalyFlag", IntegerType(), True),
    StructField("AnomalyType", StringType(), True)
])
# :white_check_mark: Paths (use subdirectories to avoid overlap with DLT system files)
raw_path = "abfss://raw@shibang.dfs.core.windows.net/raw_data/"
schema_path = "abfss://raw@shibang.dfs.core.windows.net/dlt_schemas/bronze_orders_shibang"
checkpoint_path = "abfss://raw@shibang.dfs.core.windows.net/dlt_checkpoints/bronze_orders_shibang"
@table(
    name="shibang.capstone_retail_shibang.bronze_orders",
    comment="Bronze table - raw ingested IoT retail data via Auto Loader"
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("cloudFiles.schemaLocation", schema_path)
        .schema(iot_schema)  # :white_check_mark: manually set schema (no infer needed)
        .load(raw_path)  # :white_check_mark: points to /raw_data/ folder
    )