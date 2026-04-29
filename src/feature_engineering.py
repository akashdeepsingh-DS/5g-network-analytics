from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour, date_format


spark = SparkSession.builder \
    .appName("5G Feature Engineering") \
    .getOrCreate()


df = spark.read.csv(
    "data/processed/cleaned_5g_network_data.csv",
    header=True,
    inferSchema=True
)


df_features = df \
    .withColumn("hour_of_day", hour(col("timestamp"))) \
    .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
    .withColumn(
        "signal_quality",
        when(col("signal_strength_dbm") >= -80, "Good")
        .when((col("signal_strength_dbm") < -80) & (col("signal_strength_dbm") >= -100), "Moderate")
        .otherwise("Poor")
    ) \
    .withColumn(
        "latency_category",
        when(col("latency_ms") <= 20, "Low")
        .when((col("latency_ms") > 20) & (col("latency_ms") <= 50), "Medium")
        .otherwise("High")
    ) \
    .withColumn(
        "jitter_category",
        when(col("jitter_ms") <= 2, "Low")
        .when((col("jitter_ms") > 2) & (col("jitter_ms") <= 5), "Medium")
        .otherwise("High")
    ) \
    .withColumn(
        "download_speed_category",
        when(col("download_speed_mbps") >= 700, "Very Fast")
        .when((col("download_speed_mbps") >= 300) & (col("download_speed_mbps") < 700), "Fast")
        .when((col("download_speed_mbps") >= 100) & (col("download_speed_mbps") < 300), "Moderate")
        .otherwise("Slow")
    ) \
    .withColumn(
        "is_high_congestion",
        when(col("network_congestion_level") == "High", 1).otherwise(0)
    ) \
    .withColumn(
        "is_dropped",
        when(col("dropped_connection") == True, 1).otherwise(0)
    ) \
    .withColumn(
        "is_poor_signal",
        when(col("signal_strength_dbm") < -100, 1).otherwise(0)
    ) \
    .withColumn(
        "is_high_latency",
        when(col("latency_ms") > 50, 1).otherwise(0)
    ) \
    .withColumn(
        "total_latency_ms",
        col("latency_ms") + col("ping_to_google_ms")
    ) \
    .withColumn(
        "speed_difference_mbps",
        col("download_speed_mbps") - col("upload_speed_mbps")
    )


print("Feature Engineered Data Preview:")
df_features.show(5, truncate=False)

print("New Columns Added:")
new_columns = set(df_features.columns) - set(df.columns)
print(new_columns)

print("Total Columns After Feature Engineering:", len(df_features.columns))


df_features.toPandas().to_csv(
    "data/processed/feature_engineered_5g_data.csv",
    index=False
)

print("\nFeature engineered data saved successfully:")
print("data/processed/feature_engineered_5g_data.csv")


spark.stop()