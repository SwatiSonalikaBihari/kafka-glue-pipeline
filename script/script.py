# Generated from: s3 transformation data.ipynb
# Converted at: 2025-12-17T09:23:29.711Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

# # AWS Glue Studio Notebook
# ##### You are now running a AWS Glue Studio notebook; To start using your notebook you need to start an AWS Glue Interactive Session.
# 


# #### Optional: Run this cell to see available notebook commands ("magics").
# 


# #### Example: Write the data in the DynamicFrame to a location in Amazon S3 and a table for it in the AWS Glue Data Catalog
# 


from pyspark.sql import SparkSession

# Glue notebook already provides SparkSession
spark = SparkSession.builder.getOrCreate()

database_name = "default"
table_name = "ingest_year_2025"

# Read table from Glue Data Catalog as Spark DataFrame
df = spark.read.table(f"{database_name}.{table_name}")

# Show data
df.show(truncate=False)


from pyspark.sql.functions import col, from_unixtime, to_timestamp

df_out = df.withColumn(
    "event_time",
    to_timestamp(from_unixtime(col("ts") / 1000))
).show()


s3_output_path = "s3://akhil-iceberg/script_new/"
df.write \
    .format("JSON") \
    .mode("overwrite").save(s3_output_path)