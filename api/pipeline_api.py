# ============================================
# API DATA PIPELINE (REST → DELTA)
# ============================================

import requests
import json
import unicodedata
import re
import uuid

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql.functions import (
    col, first, lit, current_timestamp,
    coalesce, udf, regexp_replace, when
)
from pyspark.sql.types import StringType
import pyspark.sql.functions as F


# ============================================
# CONFIGURATION
# ============================================

API_URL = "https://api.external-service.com/data"
CREDENTIALS_PATH = "config/api_keys.json"
TARGET_TABLE = "silver_layer.api_data"


# ============================================
# LOAD CREDENTIALS
# ============================================

def load_credentials():
    with open(CREDENTIALS_PATH, "r") as f:
        return json.load(f)

api_units = load_credentials()


# ============================================
# HTTP SESSION (WITH RETRY)
# ============================================

def create_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)

    session.mount("https://", adapter)
    return session

session = create_session()


# ============================================
# UTILITIES
# ============================================

def normalize_string(value):
    if not value:
        return ""

    value = "".join(
        c for c in unicodedata.normalize("NFD", value)
        if unicodedata.category(c) != "Mn"
    )

    value = re.sub(r"[^a-z0-9]+", "_", value.lower().strip())
    return re.sub(r"_+", "_", value).strip("_")


def extract_rows_recursive(data):
    if isinstance(data, list):

        if not data:
            return

        if isinstance(data[0], dict) and (
            "header" in data[0] or "value" in data[0]
        ):
            yield data
        else:
            for item in data:
                yield from extract_rows_recursive(item)


# ============================================
# DATA EXTRACTION
# ============================================

print("🚀 Starting API extraction...")

all_cells = []

for unit_name, creds in api_units.items():

    headers = {
        "x-api-key": creds["api_key"],
        "x-tenant-key": creds["tenant_key"]
    }

    page = 1

    while page <= 100:
        try:
            response = session.get(
                f"{API_URL}?page={page}",
                headers=headers,
                timeout=60
            )

            if response.status_code != 200:
                break

            raw = response.json()

            if not raw:
                break

            found_rows = False

            for cell_list in extract_rows_recursive(raw):

                row_id = next(
                    (c.get("row_id") for c in cell_list if c.get("row_id")),
                    None
                )

                unique_key = (
                    f"{unit_name}_{row_id}"
                    if row_id else f"{unit_name}_{uuid.uuid4()}"
                )

                for cell in cell_list:
                    if isinstance(cell, dict) and "header" in cell:

                        clean = {k.lower(): v for k, v in cell.items()}
                        clean["source_unit"] = unit_name
                        clean["unique_key"] = unique_key

                        all_cells.append(clean)
                        found_rows = True

            if not found_rows:
                break

            page += 1

        except Exception as e:
            print(f"⚠️ Error: {e}")
            break

print(f"✅ Total extracted: {len(all_cells)}")


# ============================================
# PROCESSING
# ============================================

if all_cells:

    df_raw = spark.createDataFrame(all_cells)

    # Normalize header names
    normalize_udf = udf(normalize_string, StringType())
    df_raw = df_raw.withColumn("header", normalize_udf(col("header")))

    # Pivot transformation
    df_pivot = df_raw.groupBy("unique_key", "source_unit") \
        .pivot("header") \
        .agg(first("value"))

    # Helper function
    def safe_col(df, name):
        return col(f"`{name}`") if name in df.columns else lit(None)

    # Column selection (simplified for demo)
    df_selected = df_pivot.select(
        col("source_unit"),
        safe_col(df_pivot, "activity").alias("activity"),
        safe_col(df_pivot, "creation_date").alias("created_at"),
        safe_col(df_pivot, "risk_score").alias("risk_score"),
        safe_col(df_pivot, "location").alias("location"),
        current_timestamp().alias("processing_timestamp")
    )

    # Data cleaning example
    if "risk_score" in df_selected.columns:
        df_selected = df_selected.withColumn(
            "risk_score",
            regexp_replace(col("risk_score"), ",", ".").cast("double")
        )

    # Filtering
    df_final = df_selected.filter(col("created_at").isNotNull())

    # Write output
    df_final.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TARGET_TABLE)

    print("✅ API pipeline executed successfully")

else:
    print("⚠️ No data extracted")
