from pyspark.sql.functions import col, trim

storage_account = "stmlopsdemo"
container_name = "mlops"
relative_path = "batch-inference-bundle/data/input/customers.csv" # in your Blob container

input_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{relative_path}"

catalog_name = "hive_metastore"
schema_name = "default"
bronze_table = f"{catalog_name}.{schema_name}.customers_bronze"

df_raw = (
spark.read
.option("header", "true")
.option("inferSchema", "true")
.csv(input_path)
)

df_clean = (
df_raw
.withColumn("customer_id", col("customer_id").cast("long"))
.withColumn("age", col("age").cast("int"))
.withColumn("income", col("income").cast("double"))
.withColumn("tenure_months", col("tenure_months").cast("int"))
.withColumn("region", trim(col("region")))
)

(
df_clean.write
.mode("overwrite")
.format("delta")
.saveAsTable(bronze_table)
)

print(f"Bronze table written: {bronze_table}")