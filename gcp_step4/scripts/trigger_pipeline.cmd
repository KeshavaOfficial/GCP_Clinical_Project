@echo off
if "%GCP_PROJECT_ID%"=="" (echo Set GCP_PROJECT_ID & exit /b 1)
if "%GCP_REGION%"=="" (echo Set GCP_REGION & exit /b 1)
if "%COMPOSER_ENV%"=="" (echo Set COMPOSER_ENV & exit /b 1)
echo WARNING: normal mode writes to Bronze and Silver.
set /p CONFIRM=Type RUN to continue: 
if not "%CONFIRM%"=="RUN" (echo Cancelled & exit /b 1)
gcloud composer environments run "%COMPOSER_ENV%" --location="%GCP_REGION%" --project="%GCP_PROJECT_ID%" dags trigger -- clinical_data_pipeline --conf "{\"mode\":\"normal\"}"
