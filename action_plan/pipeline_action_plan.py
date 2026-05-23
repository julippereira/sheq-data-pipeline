# ============================================
# ACTION PLAN PIPELINE (API + CSV → DELTA)
# ============================================

import requests
import json
import unicodedata

from pyspark.sql import functions as F
from pyspark.sql import Row
from functools import reduce


# ============================================
# CONFIGURATION
# ============================================

API_URL = "https://api.external-service.com/action-plans"
CSV_PATH = "/path/to/action_plan.csv"
CREDENTIALS_PATH = "/Volumes/your-volume-path/api_keys.json"
TARGET_TABLE = "silver_layer.action_plan"


# ============================================
# LOAD CREDENTIALS
# ============================================

def load_credentials():
    with open(CREDENTIALS_PATH, "r") as f:
        return json.load(f)

api_units = load_credentials()


# ============================================
# 1. API EXTRACTION
# ============================================

print("➡️ [1/5] Extracting API...")

api_data = []

for unit, creds in api_units.items():

    headers = {
        "x-api-key": creds["api_key"],
        "x-ambito-key": creds["tenant_key"]  # real header kept internally
    }

    try:
        response = requests.get(API_URL, headers=headers, timeout=30)

        if response.status_code == 200:

            for item in response.json():

                record = {
                    "plant": unit,
                    "plan_id": str(item.get("plan_id", "")),
                    "objective": str(item.get("description", "")),
                    "action": str(item.get("action", "")),
                    "owner": str(item.get("owner", "")),
                    "status": str(item.get("status", "")),
                    "due_date": str(item.get("due_date", "")),
                    "completion_date": str(item.get("completion_date", "")),
                    "updated_at": str(item.get("created_at", "")),
                    "backup_id": str(item.get("plan_id", "")),
                    "data_source": "API"
                }

                api_data.append(record)

        else:
            print(f"⚠️ API error {response.status_code} for {unit}")

    except Exception as e:
        print(f"⚠️ Error for {unit}: {e}")


df_api = spark.createDataFrame(api_data) if api_data else None


# ============================================
# 2. CSV EXTRACTION
# ============================================

print("➡️ [2/5] Reading CSV...")

try:
    df_csv = (
        spark.read
        .option("header", "true")
        .option("delimiter", ";")
        .csv(CSV_PATH)
    )

    df_csv = df_csv.withColumn("data_source", F.lit("CSV"))

    for col_name in df_csv.columns:
        df_csv = df_csv.withColumn(col_name, F.trim(F.col(col_name)))

except Exception as e:
    print(f"⚠️ CSV error: {e}")
    df_csv = None


# ============================================
# 3. NORMALIZATION & MERGE
# ============================================

print("➡️ [3/5] Normalizing and merging...")

mapping = {
    "PLANT": "plant",
    "PLAN": "plan_id",
    "OBJECTIVE": "objective",
    "ACTION": "action",
    "OWNER": "owner",
    "STATUS": "status",
    "DUE_DATE": "due_date",
    "COMPLETION_DATE": "completion_date",
    "UPDATED_AT": "updated_at",
    "DATA_SOURCE": "data_source"
}


def normalize_column(name):
    name = unicodedata.normalize("NFKD", name)
    return name.encode("ascii", "ignore").decode("utf-8").upper().strip()


mapping_norm = {normalize_column(k): v for k, v in mapping.items()}


def select_mapped(df):
    columns = {normalize_column(c): c for c in df.columns}

    return df.select([
        F.col(columns[k]).alias(v) if k in columns
        else F.lit(None).alias(v)
        for k, v in mapping_norm.items()
    ])


dfs = [df for df in [df_api, df_csv] if df is not None]

df_final = reduce(
    lambda x, y: x.unionByName(y, allowMissingColumns=True),
    [select_mapped(df) for df in dfs]
)


# ============================================
# 4. CLEANING & BUSINESS RULES
# ============================================

# Text cleanup
for col_clean in ["status", "owner"]:
    if col_clean in df_final.columns:
        df_final = df_final.withColumn(
            col_clean,
            F.trim(F.regexp_replace(F.col(col_clean), r"\s+", " "))
        )

# Plan ID fallback
df_final = df_final.withColumn(
    "plan_id",
    F.when(F.col("plan_id").isNull(), F.lit("unknown"))
    .otherwise(F.col("plan_id"))
)

# Date conversion (robust)
for col_date in ["due_date", "updated_at"]:
    if col_date in df_final.columns:
        df_final = df_final.withColumn(
            col_date,
            F.coalesce(
                F.to_timestamp(col(col_date)),
                F.expr(f"try_to_timestamp({col_date}, 'dd/MM/yyyy')")
            )
        )


# ============================================
# 5. WRITE OUTPUT
# ============================================

print("➡️ [5/5] Saving...")

df_final.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print("✅ Action Plan pipeline completed")
