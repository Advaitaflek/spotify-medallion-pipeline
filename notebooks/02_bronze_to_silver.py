# Databricks notebook source
# MAGIC %md
# MAGIC # Spotify Medallion Pipeline: Silver Layer Transformation
# MAGIC ### Component: `02_bronze_to_silver`
# MAGIC
# MAGIC **Objective:**
# MAGIC Read uncurated raw JSON records from the Bronze table, enforce an explicit schema, extract domain fields, and merge clean records idempotently into the Silver Delta table.
# MAGIC
# MAGIC **Design Highlights:**
# MAGIC * **Schema Enforcement:** Uses `from_json` with strict typing on nested elements.
# MAGIC * **Cleaning & Standardization:** Casts release dates, cleans string fields, converts milliseconds to seconds.
# MAGIC * **Idempotency:** Implements Delta `MERGE INTO` matching on `track_id` to handle re-runs without data duplication.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, ArrayType
)
from delta.tables import DeltaTable

# 1. Read Bronze raw Delta table
bronze_df = spark.table("workspace.spotify_db.bronze_tracks")

# 2. Schema definition (without popularity)
track_schema = StructType([
    StructField("track", StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("duration_ms", IntegerType(), True),
        StructField("artists", ArrayType(StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ])), True),
        StructField("album", StructType([
            StructField("name", StringType(), True),
            StructField("release_date", StringType(), True)
        ]), True)
    ]), True)
])

# 3. Parse JSON, unpack columns, and deduplicate
parsed_df = (
    bronze_df
    .withColumn("data", F.from_json(F.col("raw_payload"), track_schema))
    .select(
        F.col("data.track.id").alias("track_id"),
        F.col("data.track.name").alias("track_name"),
        F.col("data.track.artists")[0]["name"].alias("primary_artist"),
        F.col("data.track.artists")[0]["id"].alias("artist_id"),
        F.col("data.track.album.name").alias("album_name"),
        F.col("data.track.album.release_date").alias("release_date"),
        (F.col("data.track.duration_ms") / 1000).cast("int").alias("duration_sec"),
        F.col("ingestion_timestamp")
    )
    .filter(F.col("track_id").isNotNull())
    .sort(F.col("ingestion_timestamp").desc())
    .dropDuplicates(["track_id"])
)

# 4. Save to Silver Delta Table
silver_table_name = "workspace.spotify_db.silver_tracks"

if not spark.catalog.tableExists(silver_table_name):
  parsed_df.write.format("delta").mode("overwrite").saveAsTable(
      silver_table_name
  )
  print(f"Created initial Silver table: {silver_table_name}.")
else:
  target_table = DeltaTable.forName(spark, silver_table_name)
  (
      target_table.alias("target")
      .merge(parsed_df.alias("source"), "target.track_id = source.track_id")
      .whenMatchedUpdate(
          set={
              "track_name": "source.track_name",
              "primary_artist": "source.primary_artist",
              "artist_id": "source.artist_id",
              "album_name": "source.album_name",
              "release_date": "source.release_date",
              "duration_sec": "source.duration_sec",
              "ingestion_timestamp": "source.ingestion_timestamp",
          }
      )
      .whenNotMatchedInsertAll()
      .execute()
  )
  print(f"Successfully merged current batch into {silver_table_name}.")

# COMMAND ----------

display(spark.table("workspace.spotify_db.silver_tracks"))

# COMMAND ----------

