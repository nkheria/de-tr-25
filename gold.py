from dlt import table, read
import pyspark.sql.functions as F
# ===============================
# Gold Layer Tables (capstone_retail.gold)
# ===============================
# 1. Daily / Monthly Sales per Region
@table(
    name="shibang.capstone_retail_shibang.gold_sales_region",
    comment="Daily & Monthly Sales per Region"
)
def gold_sales_region():
    return (
        read("shibang.capstone_retail_shibang.silver_orders")
        .withColumn("OrderDate", F.to_date("OrderTime", "M/d/yyyy H:mm"))
        .withColumn("YearMonth", F.date_format("OrderDate", "yyyy-MM"))
        .groupBy("Region", "OrderDate", "YearMonth")
        .agg(F.sum("SalesAmount").alias("TotalSales"))
    )
# 2. Device Anomaly Trend per Store
@table(
    name="shibang.capstone_retail_shibang.gold_anomaly_trends",
    comment="Device Anomaly Trend per Store"
)
def gold_anomaly_trends():
    return (
        read("shibang.capstone_retail_shibang.silver_orders")
        .withColumn("OrderDate", F.to_date("OrderTime", "M/d/yyyy H:mm"))
        .groupBy("StoreID", "OrderDate", "DeviceType", "AnomalyType")
        .agg(F.count("*").alias("AnomalyCount"))
    )
# 3. Conversion Rate Impact when devices fail
@table(
    name="shibang.capstone_retail_shibang.gold_conversion_impact",
    comment="Conversion rate impact of device failures"
)
def gold_conversion_impact():
    df = read("shibang.capstone_retail_shibang.silver_orders")
    total_orders = df.groupBy("StoreID").agg(F.count("*").alias("TotalOrders"))
    failed_orders = (
        df.filter(df.AnomalyFlag == 1)
        .groupBy("StoreID")
        .agg(F.count("*").alias("FailedOrders"))
    )
    return (
        total_orders.join(failed_orders, "StoreID", "left")
        .withColumn("FailedOrders", F.coalesce(F.col("FailedOrders"), F.lit(0)))
        .withColumn("FailureRate", F.col("FailedOrders") / F.col("TotalOrders"))
    )
# 4. Store Tier Classification
@table(
    name="shibang.capstone_retail_shibang.gold_store_tiers",
    comment="Store tiers based on total sales (High / Medium / Low performers)"
)
def gold_store_tiers():
    df = (
        read("shibang.capstone_retail_shibang.silver_orders")
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
# 5. Weighted Average Sales vs Anomalies
@table(
    name="shibang.capstone_retail_shibang.gold_weighted_sales_anomalies",
    comment="Weighted average of sales vs anomalies per store"
)
def gold_weighted_sales_anomalies():
    df = read("shibang.capstone_retail_shibang.silver_orders")
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