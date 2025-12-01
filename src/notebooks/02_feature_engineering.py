from pyspark.sql.functions import col, when

catalog_name = "hive_metastore"
schema_name = "default"
bronze_table = f"{catalog_name}.{schema_name}.customers_bronze"
silver_table = f"{catalog_name}.{schema_name}.customers_silver"

df_bronze = spark.table(bronze_table)

df_feat = (
df_bronze
.withColumn(
"region_code",
when(col("region") == "North", 1)
.when(col("region") == "South", 2)
.when(col("region") == "East", 3)
.when(col("region") == "West", 4)
.otherwise(0)
)
# Tenure bucket
.withColumn(
"tenure_bucket",
when(col("tenure_months") < 12, "new")
.when((col("tenure_months") >= 12) & (col("tenure_months") < 36), "established")
.when((col("tenure_months") >= 36) & (col("tenure_months") < 72), "loyal")
.otherwise("veteran")
)
# Simple normalized income
.withColumn("income_norm", col("income") / 100000.0)
)

(
df_feat.write
.mode("overwrite")
.format("delta")
.saveAsTable(silver_table)
)

print(f"Silver table written: {silver_table}")