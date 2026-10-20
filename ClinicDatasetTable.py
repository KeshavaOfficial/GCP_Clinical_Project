from google.cloud import bigquery


# ==========================================================
# 1. CONFIGURATION
# ==========================================================

PROJECT_ID = "vaarahigcpproject"

DATASET_ID = "clinical_staging"

TABLE_ID = "patient_staging"

LOCATION = "US"


# ==========================================================
# 2. CREATE BIGQUERY CLIENT
# ==========================================================

client = bigquery.Client(
    project=PROJECT_ID
)


# ==========================================================
# 3. CREATE DATASET
# ==========================================================

dataset_id = f"{PROJECT_ID}.{DATASET_ID}"

dataset = bigquery.Dataset(dataset_id)

dataset.location = LOCATION


dataset = client.create_dataset(
    dataset,
    exists_ok=True
)


print(
    f"Dataset created/verified successfully: {dataset_id}"
)


# ==========================================================
# 4. DEFINE STAGING TABLE SCHEMA
# ==========================================================
# All columns are STRING intentionally.
#
# Staging should accept the source data as-is.
# PySpark will clean and convert data types later.
# ==========================================================

schema = [

    bigquery.SchemaField(
        "patient_id",
        "STRING",
        mode="NULLABLE"
    ),

    bigquery.SchemaField(
        "study_id",
        "STRING",
        mode="NULLABLE"
    ),

    bigquery.SchemaField(
        "gender",
        "STRING",
        mode="NULLABLE"
    ),

    bigquery.SchemaField(
        "age",
        "STRING",
        mode="NULLABLE"
    ),

    bigquery.SchemaField(
        "country",
        "STRING",
        mode="NULLABLE"
    ),

    bigquery.SchemaField(
        "medical_condition",
        "STRING",
        mode="NULLABLE"
    ),

    bigquery.SchemaField(
        "treatment",
        "STRING",
        mode="NULLABLE"
    ),

    bigquery.SchemaField(
        "visit_date",
        "STRING",
        mode="NULLABLE"
    ),

    bigquery.SchemaField(
        "trial_phase",
        "STRING",
        mode="NULLABLE"
    ),

    bigquery.SchemaField(
        "study_status",
        "STRING",
        mode="NULLABLE"
    ),

    bigquery.SchemaField(
        "adverse_event",
        "STRING",
        mode="NULLABLE"
    )
]


# ==========================================================
# 5. CREATE TABLE
# ==========================================================

table_id = (
    f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
)

table = bigquery.Table(
    table_id,
    schema=schema
)


table = client.create_table(
    table,
    exists_ok=True
)


print(
    f"Table created/verified successfully: {table_id}"
)


# ==========================================================
# 6. SUCCESS MESSAGE
# ==========================================================

print("\n==========================================")
print("BIGQUERY STAGING SETUP COMPLETED")
print("==========================================")

print("Project :", PROJECT_ID)
print("Dataset :", DATASET_ID)
print("Table   :", TABLE_ID)

print("==========================================")