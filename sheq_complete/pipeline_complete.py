# ============================================
# SHEQ COMPLETE PIPELINE (UNIFIED DATASET)
# ============================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime


# ============================================
# CONFIGURATION
# ============================================

TABLE_API = "silver_layer.api_data"
TABLE_HISTORICAL = "silver_layer.historical_data"
TABLE_ACTION_PLAN = "silver_layer.action_plan"
TARGET_TABLE = "gold_layer.sheq_complete"


def log_step(message):
    current_time = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"[{current_time}] {message}", flush=True)


log_step(">>> START: SHEQ COMPLETE PIPELINE")

# ============================================
# 1. READ & UNIFY DATA
# ============================================

log_step("1. Reading source tables...")

df_api = spark.read.table(TABLE_API)
df_hist = spark.read.table(TABLE_HISTORICAL)

df_api = df_api.withColumn("data_source", F.lit("API"))
df_hist = df_hist.withColumn("data_source", F.lit("CSV"))

# Convert date columns to string (avoid schema conflicts)
date_keywords = ["date", "dt_", "due"]

for col_name in df_api.columns:
    if any(k in col_name.lower() for k in date_keywords):
        df_api = df_api.withColumn(col_name, F.col(col_name).cast("string"))

for col_name in df_hist.columns:
    if any(k in col_name.lower() for k in date_keywords):
        df_hist = df_hist.withColumn(col_name, F.col(col_name).cast("string"))

df_unified = df_hist.unionByName(df_api, allowMissingColumns=True)

log_step(f"   → Total records: {df_unified.count()}")


# ============================================
# 2. DATA QUALITY FILTERS
# ============================================

log_step("2. Applying filters...")

df_filtered = df_unified.filter(
    F.col("risk_before").isNotNull()
)

# Generate plant code
df_filtered = df_filtered.withColumn(
    "plant_code",
    F.when(F.upper(F.col("plant")).contains("A"), "PLA")
     .when(F.upper(F.col("plant")).contains("B"), "PLB")
     .otherwise("UNK")
)


# ============================================
# 3. DATE NORMALIZATION
# ============================================

log_step("3. Normalizing dates...")

for col_name in df_filtered.columns:
    if any(k in col_name.lower() for k in date_keywords):
        df_filtered = df_filtered.withColumn(
            col_name,
            F.coalesce(
                F.to_timestamp(col_name),
                F.expr(f"try_to_timestamp({col_name}, 'dd/MM/yyyy')")
            )
        )


# ============================================
# 4. ID GENERATION
# ============================================

log_step("4. Creating ID...")

df_filtered = df_filtered.withColumn(
    "record_id",
    F.concat(F.col("plant_code"), F.lit("_"), F.col("item"))
)


# ============================================
# 5. BUSINESS LOGIC
# ============================================

df_filtered = df_filtered.withColumn(
    "risk",
    F.coalesce(
        F.col("risk_after"),
        F.col("risk_before")
    )
)

df_filtered = df_filtered.withColumn(
    "year_ref",
    F.year("update_date")
).withColumn(
    "month_ref",
    F.month("update_date")
)


# ============================================
# 6. DEDUPLICATION
# ============================================

log_step("6. Deduplicating...")

window_spec = Window.partitionBy("record_id", "update_date") \
    .orderBy(F.desc("update_date"))

df_dedup = df_filtered.withColumn(
    "rn",
    F.row_number().over(window_spec)
).filter(
    F.col("rn") == 1
).drop("rn")


# ============================================
# 7. JOIN ACTION PLAN
# ============================================

log_step("7. Joining Action Plan...")

df_action = spark.read.table(TABLE_ACTION_PLAN)

df_action = df_action.select(
    F.col("plan_id").alias("join_plan_id"),
    "objective",
    "action",
    "owner",
    "status"
)

df_result = df_dedup.join(
    df_action,
    df_dedup["plan_id"] == df_action["join_plan_id"],
    "left"
)


# ============================================
# 8. WRITE OUTPUT
# ============================================

log_step("8. Saving output...")

df_result.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(TARGET_TABLE)

log_step(">>> FINISHED SUCCESSFULLY")
