from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("5G Network Analytics") \
    .getOrCreate()

# Read CSV file
df = spark.read.csv(
    "data/raw/5g_network_data.csv",
    header=True,
    inferSchema=True
)

# Show first 5 rows
print("First 5 Rows:")
df.show(5, truncate=False)

# Print Schema
print("Schema:")
df.printSchema()

# Row Count
print("Total Rows:", df.count())

# Column Count
print("Total Columns:", len(df.columns))

# Column Names
print("Columns:")
print(df.columns)

# Stop Spark Session
spark.stop()