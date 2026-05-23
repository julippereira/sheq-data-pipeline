# ============================================
# SHEQ MONTHLY SNAPSHOT PIPELINE
# ============================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, IntegerType
from datetime import datetime


# ============================================
# CONFIGURATION
# ============================================

SOURCE_TABLE = "gold_layer.sheq_complete"
TARGET_TABLE = "gold_layer.sheq_monthly"

ID_COLUMN = "record_id"
FULL_LOAD = True


# ============================================
# 1. READ DATA
# ============================================

df = spark.read.table(SOURCE_TABLE)


# ============================================
# 2. INITIAL CLEANING
# ============================================

df = df.withColumn(
    "_id_clean",
    F.upper(F.trim(F.regexp_replace(F.col(ID_COLUMN), r"[\r\n\t\u00a0]", "")))
)

df = df.withColumn(
    "update_date",
    F.coalesce(
        F.col("update_date").cast(TimestampType()),
        F.to_timestamp("update_date", "yyyy-MM-dd"),
        F.to_timestamp("update_date", "dd/MM/yyyy")
    )
)

df = df.filter(
    (F.col("_id_clean").isNotNull()) &
    (F.col("update_date").isNotNull())
)

df = df.withColumn("year_ref", F.year("update_date").cast(IntegerType())) \
       .withColumn("month_ref", F.month("update_date").cast(IntegerType()))


# ============================================
# 3. DATE NORMALIZATION
# ============================================

date_keywords = ["date", "dt_", "due"]

for col_name, col_type in df.dtypes:
    if any(k in col_name.lower() for k in date_keywords):

        if col_name != "update_date":

            df = df.withColumn(
                col_name,
                F.coalesce(
                    F.col(col_name).cast(TimestampType()),
                    F.to_timestamp(col_name, "yyyy-MM-dd"),
                    F.to_timestamp(col_name, "dd/MM/yyyy")
                )
            )


# ============================================
# 4. SNAPSHOT LOGIC (LATEST PER MONTH)
# ============================================

window_spec = Window.partitionBy(
    "_id_clean", "year_ref", "month_ref"
).orderBy(
    F.col("update_date").desc()
)

df_snapshot = df.withColumn(
    "rn",
    F.row_number().over(window_spec)
).filter(
    F.col("rn") == 1
).drop("rn", "_id_clean")


# ============================================
# 5. DATA ENRICHMENT
# ============================================

# Risk classification
if "risk" in df_snapshot.columns:
    df_snapshot = df_snapshot.withColumn(
        "risk_level",
        F.when(F.col("risk").contains("Very High"), 1)
         .when(F.col("risk").contains("High"), 2)
         .when(F.col("risk").contains("Medium"), 3)
         .when(F.col("risk").contains("Low"), 4)
         .otherwise(None)
         .cast(IntegerType())
    )

# SAF code extraction
if "saf_code" in df_snapshot.columns:
    df_snapshot = df_snapshot.withColumn(
        "saf_category",
        F.substring(F.col("saf_code"), 1, 3)
    )


# ============================================
# 6. LOAD STRATEGY
# ============================================

if FULL_LOAD:
    df_final = df_snapshot
else:
    today = datetime.now()
    df_final = df_snapshot.filter(
        (F.col("year_ref") == today.year) &
        (F.col("month_ref") == today.month)
    )


# ============================================
# 7. WRITE OUTPUT
# ============================================

if not df_final.isEmpty():

    writer = df_final.write.format("delta") \
        .partitionBy("year_ref", "month_ref")

    if FULL_LOAD:
        writer.mode("overwrite") \
              .option("overwriteSchema", "true") \
              .saveAsTable(TARGET_TABLE)
    else:
        condition = f"year_ref = {today.year} AND month_ref = {today.month}"

        writer.mode("overwrite") \
              .option("mergeSchema", "true") \
              .option("replaceWhere", condition) \
              .saveAsTable(TARGET_TABLE)

    print("✅ Monthly snapshot saved successfully")

else:
    print("⚠️ No data to process")
