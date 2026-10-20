@echo off
setlocal EnableExtensions
if "%GCP_PROJECT_ID%"=="" (echo Set GCP_PROJECT_ID & exit /b 1)
if "%COMPOSER_DAGS_URI%"=="" (echo Set COMPOSER_DAGS_URI & exit /b 1)
set "ROOT=%~dp0.."
for /f "usebackq delims=" %%F in ("%ROOT%\manifest.txt") do (
 if not exist "%ROOT%\airflow\dags\%%F" (echo Missing %%F; no upload & exit /b 1)
 python -m py_compile "%ROOT%\airflow\dags\%%F" || exit /b 1
)
for /f "usebackq delims=" %%F in ("%ROOT%\manifest.txt") do (
 gcloud storage cp "%ROOT%\airflow\dags\%%F" "%COMPOSER_DAGS_URI%/%%F" --project="%GCP_PROJECT_ID%" || exit /b 1
)
echo Uploaded; no DAG triggered.
