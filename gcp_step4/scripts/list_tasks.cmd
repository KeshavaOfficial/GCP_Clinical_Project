@echo off
gcloud composer environments run "%COMPOSER_ENV%" --location="%GCP_REGION%" --project="%GCP_PROJECT_ID%" tasks list -- clinical_data_pipeline
