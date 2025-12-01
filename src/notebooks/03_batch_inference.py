from pyspark.sql.functions import col, when

catalog_name = "hive_metastore"
schema_name = "default"
silver_table = f"{catalog_name}.{schema_name}.customers_silver"
gold_table = f"{catalog_name}.{schema_name}.customers_gold"

df_silver = spark.table(silver_table)

df_scored = (
df_silver
.withColumn(
"churn_risk_score",
(
when(col("tenure_bucket") == "new", 0.8)
.when(col("tenure_bucket") == "established", 0.5)
.when(col("tenure_bucket") == "loyal", 0.3)
.otherwise(0.2)
)
+ (1.0 - col("income_norm")) * 0.2
+ (1.0 - (col("age") / 100.0)) * 0.1
)
)

(
df_scored.write
.mode("overwrite")
.format("delta")
.saveAsTable(gold_table)
)

print(f"Gold table written: {gold_table}")