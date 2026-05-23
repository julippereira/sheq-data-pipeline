# ============================================
# SHEQ STATUS PIPELINE (STATE MACHINE)
# ============================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType


# ============================================
# CONFIGURATION
# ============================================

SOURCE_TABLE = "gold_layer.sheq_monthly"
TARGET_TABLE = "gold_layer.sheq_status"


# ============================================
# 1. READ & CLEAN DATA
# ============================================

df = spark.read.table(SOURCE_TABLE)

df = df \
    .withColumn("record_id", F.regexp_replace(F.col("record_id"), r"[.,]", "")) \
    .filter(F.col("record_id").isNotNull()) \
    .filter(F.col("record_id") != "")


# ============================================
# 2. BUILD MONTH GRID (TIME SERIES COMPLETION)
# ============================================

max_month = df.select(F.max(F.trunc("update_date", "month"))).collect()[0][0]

df_base = df \
    .withColumn("month_ref", F.trunc("update_date", "month")) \
    .withColumn("is_present", F.lit(True))

df_limits = df_base.groupBy("record_id").agg(
    F.min("month_ref").alias("first_month")
).withColumn("last_month", F.lit(max_month))

df_grid = df_limits.withColumn(
    "month_ref",
    F.explode(F.expr("sequence(first_month, last_month, interval 1 month)"))
).drop("first_month", "last_month")


# ============================================
# 3. JOIN REAL DATA WITH FULL TIMELINE
# ============================================

df_timeline = df_grid.join(df_base, ["record_id", "month_ref"], "left")


# ============================================
# 4. FORWARD FILL (CONTINUOUS HISTORY)
# ============================================

window_ffill = Window.partitionBy("record_id") \
    .orderBy("month_ref") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_timeline = df_timeline \
    .withColumn("risk_filled", F.last("risk", True).over(window_ffill)) \
    .withColumn("risk_effective", F.coalesce("risk", "risk_filled")) \
    .withColumn("risk_class_filled", F.last("risk_level", True).over(window_ffill))


# ============================================
# 5. STATE MACHINE (STATUS LOGIC)
# ============================================

window_lag = Window.partitionBy("record_id").orderBy("month_ref")

df_final = df_timeline \
    .withColumn("risk_prev", F.lag("risk_effective").over(window_lag)) \
    .withColumn("risk_class_prev", F.lag("risk_class_filled").over(window_lag)) \
    .withColumn("is_present_prev", F.lag("is_present").over(window_lag)) \
    .withColumn("status_id",
        F.when(F.col("is_present").isNotNull(), "ACTIVE")
         .otherwise("INACTIVE")
    ) \
    .withColumn("status",
        F.when(F.col("is_present").isNotNull() & F.col("risk_class_prev").isNull(), "NEW")
        .when(F.col("is_present").isNotNull() & (F.col("risk_class_filled") > F.col("risk_class_prev")), "NEW")
        .when(F.col("is_present").isNotNull() & (F.col("risk_class_filled") < F.col("risk_class_prev")), "MITIGATED")
        .when(F.col("is_present").isNull() & F.col("is_present_prev").isNotNull(), "DELETED")
        .when(F.col("is_present").isNotNull() & (F.col("risk_class_filled") == F.col("risk_class_prev")), "UNCHANGED")
        .otherwise(None)
    ) \
    .withColumn("status_code",
        F.when(F.col("status") == "NEW", 1)
         .when(F.col("status") == "UNCHANGED", 2)
         .when(F.col("status") == "MITIGATED", 3)
         .when(F.col("status") == "DELETED", 4)
         .cast(IntegerType())
    ) \
    .withColumn("weight",
        F.when(F.col("status").isin("NEW", "UNCHANGED"), 1)
         .when(F.col("status").isin("MITIGATED", "DELETED"), -1)
         .otherwise(0)
         .cast(IntegerType())
    ) \
    .filter(F.col("status").isNotNull())


# ============================================
# 6. FINAL OUTPUT
# ============================================

df_output = df_final.select(
    "record_id",
    "plant",
    "month_ref",
    "risk_effective",
    "risk_class_filled",
    "status_id",
    "status",
    "status_code",
    "weight"
)

df_output.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print("✅ Status pipeline executed successfully")
