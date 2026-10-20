from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    when,
    to_date
)

import Pyspark_config as config


# ==========================================================
# 1. CREATE SPARK SESSION
# ==========================================================

spark = SparkSession.builder \
    .appName(config.SPARK_APP_NAME) \
    .getOrCreate()


print("==========================================")
print("REFINED LAYER TRANSFORMATION STARTED")
print("==========================================")


# ==========================================================
# 2. READ DATA FROM BIGQUERY STAGING
# ==========================================================

print(
    "Reading staging table:",
    config.STAGING_FULL_TABLE
)


df = spark.read \
    .format("bigquery") \
    .option(
        "table",
        config.STAGING_FULL_TABLE
    ) \
    .load()


original_count = df.count()


print(
    "Original Staging Record Count:",
    original_count
)


df.show(
    10,
    truncate=False
)


# ==========================================================
# 3. REMOVE EXACT DUPLICATES
# ==========================================================

before_count = df.count()


df = df.dropDuplicates()


after_count = df.count()


print(
    "Exact Duplicates Removed:",
    before_count - after_count
)


# ==========================================================
# 4. TRIM ALL STRING COLUMNS
# ==========================================================

for column_name, data_type in df.dtypes:

    if data_type == "string":

        df = df.withColumn(
            column_name,
            trim(col(column_name))
        )


print(
    "Leading and trailing spaces removed."
)


# ==========================================================
# 5. CONVERT BLANK STRINGS TO NULL
# ==========================================================

for column_name, data_type in df.dtypes:

    if data_type == "string":

        df = df.withColumn(

            column_name,

            when(
                col(column_name) == "",
                None
            ).otherwise(
                col(column_name)
            )
        )


print(
    "Blank values converted to NULL."
)


# ==========================================================
# 6. CONVERT TEXT COLUMNS TO LOWERCASE
# ==========================================================
#
# Do NOT lowercase IDs.
# patient_id and study_id are identifiers.
#
# ==========================================================

lowercase_columns = [

    "gender",
    "country",
    "medical_condition",
    "treatment",
    "trial_phase",
    "study_status",
    "adverse_event"

]


for column_name in lowercase_columns:

    df = df.withColumn(
        column_name,
        lower(col(column_name))
    )


print(
    "Text columns converted to lowercase."
)


# ==========================================================
# 7. CAST DATA TYPES
# ==========================================================

# AGE
# STRING -> INTEGER
#
# Example:
# "45" -> 45
#
# Invalid:
# "abc" -> NULL

df = df.withColumn(
    "age",
    col("age").cast("int")
)


# VISIT DATE
# STRING -> DATE
#
# Example:
# "2026-08-15" -> DATE

df = df.withColumn(
    "visit_date",
    to_date(
        col("visit_date"),
        "yyyy-MM-dd"
    )
)


print(
    "Schema casting completed."
)


# ==========================================================
# 8. AGE VALIDATION
# ==========================================================
#
# Practice business rule:
# Accept age between 18 and 100.
#
# ==========================================================

df = df.filter(

    (col("age") >= 18) &
    (col("age") <= 100)

)


print(
    "Invalid age records removed."
)


# ==========================================================
# 9. REMOVE NULL RECORDS
# ==========================================================

before_null_count = df.count()


df = df.dropna()


after_null_count = df.count()


print(
    "Records containing NULL removed:",
    before_null_count - after_null_count
)


# ==========================================================
# 10. BUSINESS KEY DEDUPLICATION
# ==========================================================
#
# Practice assumption:
#
# patient_id + study_id + visit_date
#
# uniquely identifies a patient visit.
#
# ==========================================================

before_business_duplicates = df.count()


df = df.dropDuplicates(
    config.BUSINESS_KEY
)


after_business_duplicates = df.count()


print(
    "Business-key duplicates removed:",
    before_business_duplicates
    - after_business_duplicates
)


# ==========================================================
# 11. SELECT REFINED COLUMNS
# ==========================================================

df = df.select(

    "patient_id",
    "study_id",
    "gender",
    "age",
    "country",
    "medical_condition",
    "treatment",
    "visit_date",
    "trial_phase",
    "study_status",
    "adverse_event"

)


# ==========================================================
# 12. FINAL DATA QUALITY SUMMARY
# ==========================================================

final_count = df.count()


print("==========================================")
print("REFINED DATA QUALITY SUMMARY")
print("==========================================")


print(
    "Staging Records:",
    original_count
)


print(
    "Final Refined Records:",
    final_count
)


print(
    "Total Records Removed:",
    original_count - final_count
)


print("==========================================")


# ==========================================================
# 13. DISPLAY FINAL SCHEMA
# ==========================================================

df.printSchema()


# ==========================================================
# 14. DISPLAY SAMPLE REFINED DATA
# ==========================================================

df.show(
    20,
    truncate=False
)


# ==========================================================
# 15. WRITE TO BIGQUERY REFINED LAYER
# ==========================================================

print(
    "Writing data to:",
    config.REFINED_FULL_TABLE
)


df.write \
    .format("bigquery") \
    .option(
        "table",
        config.REFINED_FULL_TABLE
    ) \
    .option(
        "temporaryGcsBucket",
        config.BUCKET_NAME
    ) \
    .mode("append") \
    .save()


print(
    "Successfully loaded Refined table:",
    config.REFINED_FULL_TABLE
)


# ==========================================================
# 16. STOP SPARK
# ==========================================================

spark.stop()


print("==========================================")
print("REFINED LAYER TRANSFORMATION COMPLETED")
print("==========================================")