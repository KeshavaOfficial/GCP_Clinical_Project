# ==========================================================
# GCP CONFIGURATION
# ==========================================================

PROJECT_ID = "your-gcp-project-id"

REGION = "us-central1"
BQ_LOCATION = "US"


# ==========================================================
# GCS CONFIGURATION
# ==========================================================

# Bucket name must be globally unique
BUCKET_NAME = "your-clinical-data-bucket"

# Location of CSV on the machine where Airflow is running
LOCAL_FILE_PATH = "/home/airflow/data/clinical_data.csv"

# Location/name inside GCS
GCS_FILE_NAME = "input/clinical_data.csv"


# ==========================================================
# BIGQUERY DATASETS
# ==========================================================

STAGING_DATASET = "clinical_staging"

AUDIT_DATASET = "clinical_audit"

HISTORY_DATASET = "clinical_history"

AUTH_DATASET = "clinical_auth"


# ==========================================================
# BIGQUERY TABLES
# ==========================================================

STAGING_TABLE = "patient_staging"

AUDIT_TABLE = "pipeline_audit"

HISTORY_TABLE = "patient_history"

AUTH_VIEW = "patient_view"


# ==========================================================
# AIRFLOW DAG CONFIGURATION
# ==========================================================

DAG_ID = "clinical_data_pipeline"

# None = Manual trigger
SCHEDULE = None

START_DATE_YEAR = 2026
START_DATE_MONTH = 1
START_DATE_DAY = 1