# Databricks notebook source
# MAGIC %md
# MAGIC # Spotify Medallion Pipeline: Gold Layer Aggregation
# MAGIC ### Component: `03_silver_to_gold`
# MAGIC
# MAGIC **Objective:**
# MAGIC Compute business-level Key Performance Indicators (KPIs) grouped by artist to serve BI dashboards and reporting views.
# MAGIC
# MAGIC **Metrics Calculated:**
# MAGIC * Total charted tracks per artist
# MAGIC * Average song duration (seconds)
# MAGIC * Historical release span (earliest and latest releases)

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Read clean records from Silver
silver_df = spark.table("workspace.spotify_db.silver_tracks")

# 2. Compute aggregated KPIs per artist
gold_summary = (
    silver_df.groupBy("primary_artist")
    .agg(
        F.count("track_id").alias("total_charted_tracks"),
        F.round(F.avg("duration_sec"), 1).alias("avg_duration_sec"),
        F.min("release_date").alias("earliest_release"),
        F.max("release_date").alias("latest_release")
    )
    .orderBy(F.col("total_charted_tracks").desc(), F.col("avg_duration_sec").desc())
)

# 3. Save to Gold Delta Table (Overwrite ensures it reflects the latest aggregated snapshot)
gold_table_name = "workspace.spotify_db.gold_artist_summary"
gold_summary.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(gold_table_name)

print(f"Successfully generated Gold summary table: {gold_table_name}")

# COMMAND ----------

display(spark.table("workspace.spotify_db.gold_artist_summary"))

# COMMAND ----------

