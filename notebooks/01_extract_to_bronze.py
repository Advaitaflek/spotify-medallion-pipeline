# Databricks notebook source
# MAGIC %md
# MAGIC # Spotify Medallion Pipeline: Bronze Layer Ingestion
# MAGIC ### Component: `01_extract_to_bronze`
# MAGIC
# MAGIC **Objective:**
# MAGIC Extract catalog track data from the Spotify Web API using OAuth 2.0 Client Credentials and append the raw payload into a Delta Lake Bronze table.
# MAGIC
# MAGIC **Design Highlights:**
# MAGIC * **Authentication:** Encodes client credentials using base64 for token exchange.
# MAGIC * **Extraction:** Paginates through `/v1/search` with deduplication and defensive limits.
# MAGIC * **Auditability:** Preserves raw JSON unaltered alongside ingestion timestamps and search parameters.

# COMMAND ----------

# DBTITLE 1,Extract Spotify tracks to Bronze
import base64
import json
import time
import requests
from pyspark.sql import functions as F

# 1. Fetch values from Databricks Widgets
# Safe parameter reading for both interactive and job runs
try:
    client_id = dbutils.widgets.get("spotify_client_id").strip()
except Exception:
    dbutils.widgets.text("spotify_client_id", "", "Spotify Client ID")
    client_id = dbutils.widgets.get("spotify_client_id").strip()

try:
    client_secret = dbutils.widgets.get("spotify_client_secret").strip()
except Exception:
    dbutils.widgets.text("spotify_client_secret", "", "Spotify Client Secret")
    client_secret = dbutils.widgets.get("spotify_client_secret").strip()

if not client_id or not client_secret:
    raise ValueError("Please enter your Spotify Client ID and Secret in the widgets above!")

# 2. Authenticate using OAuth 2.0 Client Credentials Flow
auth_url = "https://accounts.spotify.com/api/token"
encoded_creds = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
auth_headers = {
    "Authorization": f"Basic {encoded_creds}",
    "Content-Type": "application/x-www-form-urlencoded"
}
auth_data = {"grant_type": "client_credentials"}

auth_res = requests.post(auth_url, headers=auth_headers, data=auth_data, timeout=10)
if auth_res.status_code != 200:
    raise RuntimeError(f"Spotify Authentication failed [{auth_res.status_code}]: {auth_res.text}")

access_token = auth_res.json()["access_token"]
print("Successfully authenticated with Spotify API.")

# 3. Paged Extraction from Spotify Catalog Search
# Spotify development-mode apps are capped at ~5 results per search page for
# year-filter queries, and each offset returns a different sample. We paginate
# with deduplication by track id until we reach the target count.
search_url = "https://api.spotify.com/v1/search"
api_headers = {"Authorization": f"Bearer {access_token}"}
TARGET_TRACKS = 50
MAX_PAGES = 30  # hard stop so the loop can never run forever

all_tracks = []
seen_ids = set()
offset = 0
pages = 0

print(f"Extracting {TARGET_TRACKS} tracks via paginated search...")
while len(seen_ids) < TARGET_TRACKS and pages < MAX_PAGES:
    params = {
        "q": "year:2024-2026",
        "type": "track",
        "limit": 10,
        "offset": offset,
        "market": "US"
    }
    
    res = requests.get(search_url, headers=api_headers, params=params, timeout=10)
    if res.status_code != 200:
        raise RuntimeError(f"Spotify API search failed at offset {offset} [{res.status_code}]: {res.text}")
    
    batch = res.json().get("tracks", {}).get("items", [])
    for track in batch:
        if track["id"] not in seen_ids:
            seen_ids.add(track["id"])
            all_tracks.append(track)

    offset += 10
    pages += 1
    time.sleep(0.1)  # Gentle spacing to avoid rate limiting

if len(all_tracks) < TARGET_TRACKS:
    print(f"Warning: only {len(all_tracks)} unique tracks collected after {pages} pages (Spotify dev-mode limits).")

print(f"Total tracks collected: {len(all_tracks)}")

# 4. Convert items into PySpark DataFrame
payload_rows = [{"raw_payload": json.dumps({"track": track})} for track in all_tracks]
bronze_df = (
    spark.createDataFrame(payload_rows)
         .withColumn("ingestion_timestamp", F.current_timestamp())
         .withColumn("source_query", F.lit("year:2024-2026"))
)

# 5. Append raw records to Bronze Delta Table in Unity Catalog
target_table = "workspace.spotify_db.bronze_tracks"
bronze_df.write \
         .format("delta") \
         .mode("append") \
         .saveAsTable(target_table)

print(f"Successfully appended {len(all_tracks)} records to {target_table}.")