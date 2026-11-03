# Databricks notebook source
# MAGIC %md
# MAGIC # Spotify Medallion Pipeline: Master Orchestrator
# MAGIC ### Component: `00_master_orchestrator`
# MAGIC
# MAGIC **Objective:**
# MAGIC Run the complete Medallion pipeline sequentially via `dbutils.notebook.run()`, managing credential propagation and measuring runtimes across tasks.

# COMMAND ----------

# 1. UI Widgets for Credential Injection
dbutils.widgets.text("spotify_client_id", "", "Spotify Client ID")
dbutils.widgets.text("spotify_client_secret", "", "Spotify Client Secret")

# COMMAND ----------

# Master Orchestrator: End-to-End Medallion Pipeline Runner
import time

client_id = dbutils.widgets.get("spotify_client_id").strip()
client_secret = dbutils.widgets.get("spotify_client_secret").strip()

if not client_id or not client_secret:
    raise ValueError("Please provide your Spotify Client ID and Secret in the widgets above before triggering the pipeline!")

pipeline_start_time = time.time()
print("=" * 60)
print("STARTING SPOTIFY MEDALLION ETL PIPELINE EXECUTION")
print("=" * 60)

# STEP 1: Execute Bronze Ingestion
print("\n>>> [1/3] Triggering Bronze Layer Ingestion (01_extract_to_bronze)...")
step1_start = time.time()
dbutils.notebook.run(
    "01_extract_to_bronze",
    timeout_seconds=300,
    arguments={
        "spotify_client_id": client_id,
        "spotify_client_secret": client_secret
    }
)
print(f"✓ Bronze layer completed in {round(time.time() - step1_start, 2)} seconds.")

# STEP 2: Execute Silver Cleansing & Upsert
print("\n>>> [2/3] Triggering Silver Layer Cleansing & Upsert (02_bronze_to_silver)...")
step2_start = time.time()
dbutils.notebook.run(
    "02_bronze_to_silver",
    timeout_seconds=300
)
print(f"✓ Silver layer completed in {round(time.time() - step2_start, 2)} seconds.")

# STEP 3: Execute Gold KPI Aggregations
print("\n>>> [3/3] Triggering Gold Layer Aggregations (03_silver_to_gold)...")
step3_start = time.time()
dbutils.notebook.run(
    "03_silver_to_gold",
    timeout_seconds=300
)
print(f"✓ Gold layer completed in {round(time.time() - step3_start, 2)} seconds.")

print("\n" + "=" * 60)
print(f"PIPELINE RUN FINISHED SUCCESSFULLY IN {round(time.time() - pipeline_start_time, 2)} SECONDS!")
print("=" * 60)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Bronze Raw Tracks' AS layer, count(*) AS total_records FROM workspace.spotify_db.bronze_tracks
# MAGIC UNION ALL
# MAGIC SELECT 'Silver Cleaned Tracks' AS layer, count(*) AS total_records FROM workspace.spotify_db.silver_tracks
# MAGIC UNION ALL
# MAGIC SELECT 'Gold Artist Summary' AS layer, count(*) AS total_records FROM workspace.spotify_db.gold_artist_summary;