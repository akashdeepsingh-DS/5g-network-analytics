from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


spark = SparkSession.builder \
    .appName("5G Network Data Cleaning") \
    .getOrCreate()


df = spark.read.csv(
    "data/raw/5g_network_data.csv",
    header=True,
    inferSchema=True
)


# Rename columns to clean snake_case format
df_clean = df \
    .withColumnRenamed("Timestamp", "timestamp") \
    .withColumnRenamed("Location", "location") \
    .withColumnRenamed("Signal Strength (dBm)", "signal_strength_dbm") \
    .withColumnRenamed("Download Speed (Mbps)", "download_speed_mbps") \
    .withColumnRenamed("Upload Speed (Mbps)", "upload_speed_mbps") \
    .withColumnRenamed("Latency (ms)", "latency_ms") \
    .withColumnRenamed("Jitter (ms)", "jitter_ms") \
    .withColumnRenamed("Network Type", "network_type") \
    .withColumnRenamed("Device Model", "device_model") \
    .withColumnRenamed("Carrier", "carrier") \
    .withColumnRenamed("Band", "band") \
    .withColumnRenamed("Battery Level (%)", "battery_level_percent") \
    .withColumnRenamed("Temperature (°C)", "temperature_celsius") \
    .withColumnRenamed("Connected Duration (min)", "connected_duration_min") \
    .withColumnRenamed("Handover Count", "handover_count") \
    .withColumnRenamed("Data Usage (MB)", "data_usage_mb") \
    .withColumnRenamed("Video Streaming Quality", "video_streaming_quality") \
    .withColumnRenamed("VoNR Enabled", "vonr_enabled") \
    .withColumnRenamed("Network Congestion Level", "network_congestion_level") \
    .withColumnRenamed("Ping to Google (ms)", "ping_to_google_ms") \
    .withColumnRenamed("Dropped Connection", "dropped_connection")


print("Cleaned Column Names:")
print(df_clean.columns)


print("\nMissing Values Count:")
missing_values = df_clean.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in df_clean.columns
])
missing_values.show(truncate=False)


before_count = df_clean.count()
df_clean = df_clean.dropDuplicates()
after_count = df_clean.count()

print(f"\nRows before removing duplicates: {before_count}")
print(f"Rows after removing duplicates: {after_count}")
print(f"Duplicate rows removed: {before_count - after_count}")


print("\nCleaned Data Preview:")
df_clean.show(5, truncate=False)


# Save cleaned data using pandas to avoid Windows winutils issue
df_clean.toPandas().to_csv(
    "data/processed/cleaned_5g_network_data.csv",
    index=False
)

print("\nCleaned data saved successfully in data/processed/cleaned_5g_network_data.csv")

spark.stop()