from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum as spark_sum, round


spark = SparkSession.builder \
    .appName("5G KPI Analysis") \
    .getOrCreate()


df = spark.read.csv(
    "data/processed/feature_engineered_5g_data.csv",
    header=True,
    inferSchema=True
)


# Overall KPI Summary
overall_kpi = df.agg(
    count("*").alias("total_records"),
    round(avg("download_speed_mbps"), 2).alias("avg_download_speed_mbps"),
    round(avg("upload_speed_mbps"), 2).alias("avg_upload_speed_mbps"),
    round(avg("latency_ms"), 2).alias("avg_latency_ms"),
    round(avg("jitter_ms"), 2).alias("avg_jitter_ms"),
    round(avg("signal_strength_dbm"), 2).alias("avg_signal_strength_dbm"),
    round(avg("ping_to_google_ms"), 2).alias("avg_ping_to_google_ms"),
    round((spark_sum("is_dropped") / count("*")) * 100, 2).alias("dropped_connection_rate_percent")
)

print("\nOverall KPI Summary:")
overall_kpi.show(truncate=False)


# KPI by Network Type
kpi_by_network = df.groupBy("network_type").agg(
    count("*").alias("total_records"),
    round(avg("download_speed_mbps"), 2).alias("avg_download_speed_mbps"),
    round(avg("upload_speed_mbps"), 2).alias("avg_upload_speed_mbps"),
    round(avg("latency_ms"), 2).alias("avg_latency_ms"),
    round(avg("jitter_ms"), 2).alias("avg_jitter_ms"),
    round((spark_sum("is_dropped") / count("*")) * 100, 2).alias("drop_rate_percent")
)

print("\nKPI by Network Type:")
kpi_by_network.show(truncate=False)


# KPI by Carrier
kpi_by_carrier = df.groupBy("carrier").agg(
    count("*").alias("total_records"),
    round(avg("download_speed_mbps"), 2).alias("avg_download_speed_mbps"),
    round(avg("latency_ms"), 2).alias("avg_latency_ms"),
    round(avg("signal_strength_dbm"), 2).alias("avg_signal_strength_dbm"),
    round((spark_sum("is_dropped") / count("*")) * 100, 2).alias("drop_rate_percent")
)

print("\nKPI by Carrier:")
kpi_by_carrier.show(truncate=False)


# KPI by Location
kpi_by_location = df.groupBy("location").agg(
    count("*").alias("total_records"),
    round(avg("download_speed_mbps"), 2).alias("avg_download_speed_mbps"),
    round(avg("latency_ms"), 2).alias("avg_latency_ms"),
    round(avg("jitter_ms"), 2).alias("avg_jitter_ms"),
    round((spark_sum("is_dropped") / count("*")) * 100, 2).alias("drop_rate_percent")
)

print("\nKPI by Location:")
kpi_by_location.show(truncate=False)


# Save KPI outputs for Power BI
overall_kpi.toPandas().to_csv("data/output/overall_kpi.csv", index=False)
kpi_by_network.toPandas().to_csv("data/output/kpi_by_network_type.csv", index=False)
kpi_by_carrier.toPandas().to_csv("data/output/kpi_by_carrier.csv", index=False)
kpi_by_location.toPandas().to_csv("data/output/kpi_by_location.csv", index=False)

print("\nKPI analysis files saved successfully in data/output/")


spark.stop()