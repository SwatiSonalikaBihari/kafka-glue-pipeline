from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp
spark = SparkSession.builder.getOrCreate()
database_name = "default"
table_name = "ingest_year_2025"
df = spark.read.table(f"{database_name}.{table_name}")
df.show(truncate=False)
print("New__Feature")

df_out = df.withColumn(
    "event_time",
    to_timestamp(from_unixtime(col("ts") / 1000))
).show()


s3_output_path = "s3://akhil-iceberg/transformed_data-new/"
df.write \
    .format("JSON") \
    .mode("overwrite").save(s3_output_path)
