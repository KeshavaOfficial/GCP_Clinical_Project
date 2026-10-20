# Step 3 — Repository security checklist

## Completed for the four supplied Python files
- Created sanitized **repository copies**; live Composer files are unchanged.
- Replaced hardcoded GCP project IDs with `GCP_PROJECT_ID` from environment.
- Made datasets configurable; removed the historical default batch ID.
- Preserved existing DAG imports and flat Composer DAG layout in these copies.
- Added `.gitignore` and non-secret `.env.example`.

## Before publishing
1. Add all remaining source modules to this repository and inspect them for API tokens, passwords, private URLs, patient data, and credentials. The current package contains only four Python modules; it is **not** a complete deployable DAG.
2. Do not commit real patient data, generated CSV batches, logs, or service-account JSON.
3. Run `python security/check_secrets.py` and review any flagged matches. This is a basic scanner, **not** a guarantee of safety. Also run `gitleaks detect --source .` if installed.
4. Scan **Git history**, not only current files. If credentials were ever committed, revoke/rotate them and purge history as appropriate.
5. Configure Composer environment variables / secret references before deploying sanitized copies. Do **not** replace working DAGs directly from this partial repository.
6. Use Google Cloud service-account identity / Application Default Credentials; do not embed service-account keys. Apply least-privilege IAM for GCS, BigQuery and Cloud Run.
7. Treat patient identifiers and clinical records as sensitive; synthetic examples only in public GitHub.
8. Add real unit tests and CI checks before calling the repository production-ready.

## Why no blind refactor
Current Airflow DAG imports helper modules by their existing flat names. Moving them to `airflow/include` without packaging/import changes would break deployment. The sanitized copies retain these imports.
