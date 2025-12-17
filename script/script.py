from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp, when

# Glue notebook already provides SparkSession
spark = SparkSession.builder.getOrCreate()

database_name = "default"
table_name = "ingest_year_2025"

# Read table from Glue Data Catalog
df = spark.read.table(f"{database_name}.{table_name}")

print("🔹 Source Data")
df.show(truncate=False)

# -------------------------------
# Small & Simple Transformation
# -------------------------------

df_transformed = (
    df
    # Convert epoch milliseconds to timestamp
    .withColumn(
        "event_time",
        to_timestamp(from_unixtime(col("ts") / 1000))
    )
    # Add a simple derived column
    .withColumn(
        "amount_category",
        when(col("amount") < 100, "LOW")
        .when((col("amount") >= 100) & (col("amount") <= 500), "MEDIUM")
        .otherwise("HIGH")
    )
)

print("🔹 Transformed Data")
df_transformed.show(truncate=False)

print("CI/CD test - version 2")

# -------------------------------
# Write ORIGINAL data
# -------------------------------
s3_output_raw = "s3://akhil-iceberg/script_new/raw/"

df.write \
    .mode("overwrite") \
    .json(s3_output_raw)

# -------------------------------
# Write TRANSFORMED data
# -------------------------------
s3_output_transformed = "s3://akhil-iceberg/script_new/transformed/"

df_transformed.write \
    .mode("overwrite") \
    .json(s3_output_transformed)
