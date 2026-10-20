# Step 4: Deployment Scripts (Infrastructure Automation)

These scripts deploy the **existing** GCP clinical pipeline to an **already provisioned** Cloud Composer environment. They do not create GCP resources (no Terraform), change IAM, unpause the DAG, or trigger it during deployment.

## Why deployment scripts instead of Terraform now?
The project resources already exist. Scripted deployment is the lowest-risk way to make updates repeatable without rebuilding the environment. Terraform could be added later for fresh environments.

## Windows CMD setup
From the repository root:

```cmd
set GCP_PROJECT_ID=vaarahigcpproject
set COMPOSER_ENV=paraxelairflow
set GCP_REGION=us-central1
set COMPOSER_DAGS_URI=gs://us-central1-paraxelairflow-aae43654-bucket/dags
scripts\deploy_composer.cmd
scripts\list_tasks.cmd
```

Copy all actual existing Python files listed in `manifest.txt` into `airflow\dags\` first. The script checks that all files exist and compile before uploading any. **Do not put placeholder modules there.**

To intentionally start a normal DAG run (writes to Bronze and Silver):

```cmd
scripts\trigger_pipeline.cmd
```

The trigger requires typing `RUN`. Check for an active run first to avoid duplicate processing. `max_active_runs=1` serializes runs but does not prevent queuing duplicates.

## Architecture and safety

The current DAG imports helpers by bare module names (`from bronze_to_silver import ...`), so they must be deployed alongside `clinical_data_pipeline.py` in Composer's `dags/` directory. The proposed `airflow/include/` repo layout needs an import/packaging refactor before live use.

Credentials are provided by your existing `gcloud` authentication and Composer's service account, not stored in the repository. Never commit service-account JSON files, private patient data, or `.env` secrets. The project ID and bucket URI are configuration, not credentials.

## Scope
- Included: deterministic manifest, compile checks, GCS upload, task listing, manually confirmed trigger.
- Not included: Terraform provisioning, CI/CD, rollback automation, secret provisioning, or deployment to a new environment.
- **Status:** Templates prepared; not executed against your GCP project.
