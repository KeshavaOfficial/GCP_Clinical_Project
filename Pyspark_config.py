# ==========================================================
# SPARK CONFIGURATION
# ==========================================================

SPARK_APP_NAME = "ClinicalRefinedTransformation"


# ==========================================================
# GCP CONFIGURATION
# ==========================================================

PROJECT_ID = "vaarahigcpproject"

REGION = "us-central1"


# ==========================================================
# GCS CONFIGURATION
# Used as temporary storage by Spark BigQuery Connector
# ==========================================================

BUCKET_NAME = "cliniccustomerbucket"


# ==========================================================
# BIGQUERY STAGING LAYER
# ==========================================================

STAGING_DATASET = "clinical_staging"

STAGING_TABLE = "patient_staging"

STAGING_FULL_TABLE = (
    f"{PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLE}"
)


# ==========================================================
# BIGQUERY REFINED LAYER
# ==========================================================

REFINED_DATASET = "clinical_refined"

REFINED_TABLE = "patient_cleaned"

REFINED_FULL_TABLE = (
    f"{PROJECT_ID}.{REFINED_DATASET}.{REFINED_TABLE}"
)


# ==========================================================
# BUSINESS KEY
# ==========================================================

BUSINESS_KEY = [
    "patient_id",
    "study_id",
    "visit_date"
]
