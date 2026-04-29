import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, sum as spark_sum, round

os.makedirs("data/output", exist_ok=True)

spark = SparkSession.builder \
    .appName("5G Root Cause Analysis") \
    .getOrCreate()

df = spark.read.csv(
    "data/processed/feature_engineered_5g_data.csv",
    header=True,
    inferSchema=True
)

# 1. Dropped connection by congestion
congestion_analysis = df.groupBy("network_congestion_level").agg(
    count("*").alias("total_records"),
    round((spark_sum("is_dropped") / count("*")) * 100, 2).alias("drop_rate_percent"),
    round(avg("download_speed_mbps"), 2).alias("avg_download_speed"),
    round(avg("latency_ms"), 2).alias("avg_latency")
)

print("\nDropped Connections by Congestion:")
congestion_analysis.show()

# 2. Dropped connection by signal quality
signal_analysis = df.groupBy("signal_quality").agg(
    count("*").alias("total_records"),
    round((spark_sum("is_dropped") / count("*")) * 100, 2).alias("drop_rate_percent"),
    round(avg("latency_ms"), 2).alias("avg_latency")
)

print("\nDropped Connections by Signal Quality:")
signal_analysis.show()

# 3. Dropped connection by carrier
carrier_analysis = df.groupBy("carrier").agg(
    count("*").alias("total_records"),
    round((spark_sum("is_dropped") / count("*")) * 100, 2).alias("drop_rate_percent")
)

print("\nDropped Connections by Carrier:")
carrier_analysis.show()

# 4. Worst locations by latency
location_analysis = df.groupBy("location").agg(
    round(avg("latency_ms"), 2).alias("avg_latency"),
    round(avg("download_speed_mbps"), 2).alias("avg_download_speed")
).orderBy("avg_latency", ascending=False)

print("\nWorst Locations by Latency:")
location_analysis.show()

# Save files
congestion_analysis.toPandas().to_csv("data/output/rootcause_congestion.csv", index=False)
signal_analysis.toPandas().to_csv("data/output/rootcause_signal.csv", index=False)
carrier_analysis.toPandas().to_csv("data/output/rootcause_carrier.csv", index=False)
location_analysis.toPandas().to_csv("data/output/rootcause_location.csv", index=False)

print("\nRoot cause files saved in data/output/")

spark.stop()