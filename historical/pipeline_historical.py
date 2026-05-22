# ============================================
# HISTORICAL DATA PIPELINE (SANITIZED)
# ============================================

from pyspark.sql import functions as F
import unicodedata
import re

# --- CONFIG ---
INPUT_PATH = "/path/to/sample_data.csv"
TARGET_TABLE = "silver_layer.historical_data"

# --- READ CSV ---
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("sep", ";") \
    .load(INPUT_PATH)

# --- NORMALIZE COLUMNS ---
def normalize_column(name):
    name = name.lower()
    name = "".join(
        c for c in unicodedata.normalize('NFD', name)
        if unicodedata.category(c) != 'Mn'
    )
    name = re.sub(r'[^a-z0-9]+', '_', name)
    return name.strip("_")

for col_name in df.columns:
    df = df.withColumnRenamed(col_name, normalize_column(col_name))

# --- DATA CLEANING ---
df = df.withColumn("data_source", F.lit("CSV"))

# --- EXAMPLE NUMERIC CLEANING ---
numeric_cols = [c for c in df.columns if "score" in c.lower()]

for col_name in numeric_cols:
    df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), ",", "."))
    df = df.withColumn(col_name, F.col(col_name).cast("double"))

# --- DATE HANDLING ---
for col_name in df.columns:
    if "date" in col_name:
        df = df.withColumn(
            col_name,
            F.to_timestamp(F.col(col_name))
        )

# --- WRITE OUTPUT ---
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(TARGET_TABLE)

print("✅ Historical pipeline executed successfully")
