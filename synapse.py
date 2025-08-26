# JDBC parameters
jdbc_url = "shibangsynapse.sql.azuresynapse.net:1433;database=iot_data_db;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"
connection_props = {
  "user": "sqladminuser",
  "password": "SQL@shibang",
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
# Load processed view
df_synapse = spark.read.jdbc(
  url=jdbc_url,
  table="dbo.V_LossOrders",
  properties=connection_props
)
df_synapse.display()
 from dlt import table
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
# Schema for downstream use (optional, since JDBC infers schema)
iot_schema = StructType([
  StructField("OrderID", StringType(), True),
  StructField("CustomerID", StringType(), True),
  StructField("Region", StringType(), True),
  StructField("StoreID", StringType(), True),
  StructField("Product", StringType(), True),
  StructField("Quantity", IntegerType(), True),
  StructField("UnitPrice", DoubleType(), True),
  StructField("SalesAmount", DoubleType(), True),
  StructField("OrderTime", StringType(), True),   # may need casting in silver
  StructField("DeviceType", StringType(), True),
  StructField("Temperature", DoubleType(), True),
  StructField("DeviceStatus", StringType(), True),
  StructField("AnomalyFlag", IntegerType(), True),
  StructField("AnomalyType", StringType(), True)
])
# JDBC connection details for Synapse
jdbc_url = ""
connection_props = {
  "user": "sqladminuser",
  "password": "SQL@shibang",
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
@table(
  name="shibang_catalog_project.capstone_retail_shibang.bronze_orders",
  comment="Bronze table - ingested IoT retail data from Synapse via JDBC"
)
def bronze_orders():
  # Read the data from Synapse table/view dbo.V_LossOrders
  df = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.V_LossOrders",
    properties=connection_props
  )
  # Optional: if needed, select and cast columns to expected types here
  # For example, cast OrderTime to string if it's timestamp, etc.
  return df
@dlt.table(
  comment="Silver table - cleaned & typed data",
  partition_cols=["Region", "OrderDate"] # partitioning on Region and OrderDate
)
def silver_orders():
  df = dlt.read("shibang_catalog_project.capstone_retail_shibang.bronze_orders")
  return (
    df.withColumn("Quantity", F.col("Quantity").cast("int"))
     .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
     .withColumn("OrderTime", F.to_timestamp("OrderTime"))
     .withColumn("OrderDate", F.to_date("OrderTime")) # create OrderDate column for partitioning
  )
@dlt.table(
  comment="Gold table - daily and monthly sales per region"
)
def gold_sales_region():
  df = dlt.read("silver_orders") # :point_left: Simple internal reference
  return (
    df.withColumn("OrderDate", F.to_date("OrderTime", "M/d/yyyy H:mm"))
     .withColumn("YearMonth", F.date_format("OrderDate", "yyyy-MM"))
     .groupBy("Region", "OrderDate", "YearMonth")
     .agg(F.sum("SalesAmount").alias("TotalSales"))
  )
@dlt.table(
  comment="Gold table - device anomaly trends per store"
)
def gold_anomaly_trends():
  df = dlt.read("silver_orders") # :point_left: Use internal table name
  return (
    df.withColumn("OrderDate", F.to_date("OrderTime", "M/d/yyyy H:mm"))
     .groupBy("StoreID", "OrderDate", "DeviceType", "AnomalyType")
     .agg(F.count("*").alias("AnomalyCount"))
  )
@dlt.table(
  comment="Gold table 3 - Conversion rate impact of device failures"
)
def gold_conversion_impact():
  df = dlt.read("silver_orders")
  total_orders = df.groupBy("StoreID").agg(F.count("*").alias("TotalOrders"))
  failed_orders = (
    df.filter(F.col("AnomalyFlag") == 1)
     .groupBy("StoreID")
     .agg(F.count("*").alias("FailedOrders"))
  )
  return (
    total_orders.join(failed_orders, "StoreID", "left")
    .withColumn("FailedOrders", F.coalesce(F.col("FailedOrders"), F.lit(0)))
    .withColumn("FailureRate", F.col("FailedOrders") / F.col("TotalOrders"))
  )
@dlt.table(
  comment="Gold table 4 - Store tiers based on total sales (High / Medium / Low performers)"
)
def gold_store_tiers():
  df = (
    dlt.read("silver_orders")
    .groupBy("StoreID")
    .agg(F.sum("SalesAmount").alias("TotalSales"))
  )
  return (
    df.withColumn(
      "StoreTier",
      F.when(F.col("TotalSales") > 100000, "High")
       .when(F.col("TotalSales") > 50000, "Medium")
       .otherwise("Low")
    )
  )
@dlt.table(
  comment="Gold table 5 - Weighted average of sales vs anomalies per store"
)
def gold_weighted_sales_anomalies():
  df = dlt.read("silver_orders")
  return (
    df.groupBy("StoreID")
    .agg(
      F.sum("SalesAmount").alias("TotalSales"),
      F.sum(F.when(F.col("AnomalyFlag") == 1, 1).otherwise(0)).alias("TotalAnomalies")
    )
    .withColumn(
      "SalesPerAnomaly",
      F.when(F.col("TotalAnomalies") > 0, F.col("TotalSales") / F.col("TotalAnomalies"))
       .otherwise(F.lit(None))
    )
  )