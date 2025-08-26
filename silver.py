from dlt import table
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType
# =========================================
# Silver Layer - Cleaned IoT Orders
# =========================================
@table(
    name="shibang.capstone_retail_shibang.silver_orders",
    comment="Silver layer: cleaned and deduplicated IoT orders with anomaly handling"
)
def silver_orders():
    # Read Bronze
    orders = dlt.read("bronze_orders")
    # Standardize schema
    orders = (
        orders.withColumn("Quantity", F.col("Quantity").cast(IntegerType()))
        .withColumn("UnitPrice", F.col("UnitPrice").cast(DoubleType()))
        .withColumn("SalesAmount", F.col("SalesAmount").cast(DoubleType()))
        .withColumn("Temperature", F.col("Temperature").cast(DoubleType()))
        .withColumn("OrderTime", F.to_timestamp("OrderTime", "M/d/yyyy H:mm"))
    )
    # Deduplicate by OrderID
    orders = orders.dropDuplicates(["OrderID"])
    # Handle missing anomaly type
    orders = orders.withColumn(
        "AnomalyType",
        F.when((F.col("AnomalyFlag") == 1) & (F.col("AnomalyType").isNull()), "Unknown")
        .when((F.col("AnomalyFlag") == 1) & (F.col("AnomalyType") == ""), "Unknown")
        .otherwise(F.col("AnomalyType"))
    )
    # Add business rule fields
    orders = (
        orders.withColumn("IsAnomaly", F.col("AnomalyFlag") == 1)
        .withColumn("DeviceStatus", F.upper(F.col("DeviceStatus")))
        .withColumn("NormalizedStoreID", F.upper(F.col("StoreID")))
        .withColumn("NormalizedProduct", F.upper(F.col("Product")))
    )
    return orders