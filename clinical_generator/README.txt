Updated Cloud Run incremental generator for vaarahigcpproject.

Files: main.py, Dockerfile, requirements.txt
Keep your original main_backup.py separately; do not overwrite it.

Before deploying:
1. Confirm all 12 Bronze tables and configured columns exist and source fields are STRING.
2. Grant the Cloud Run execution service account BigQuery Data Viewer on Bronze,
   BigQuery Job User on project, and GCS object write permission.
3. Check downstream staging/compare DAG expects clinical_source/<BATCH_ID>/<table>.csv.
4. This generates synthetic test data by cloning rows for new IDs; it does not
   guarantee clinical referential integrity for newly created records.
5. Missing existing rows are omitted from the incoming snapshot; do not treat
   omission as deletion unless your pipeline explicitly implements that rule.
6. Use unique BATCH_ID for each new run; reruns with the same ID reuse output names.

Windows CMD from the folder containing Dockerfile:
gcloud builds submit --tag us-central1-docker.pkg.dev/vaarahigcpproject/cloud-run-source-deploy/clinical-data-generator:incremental-v2 --project=vaarahigcpproject

After build succeeds:
gcloud run jobs update clinical-data-generator --image=us-central1-docker.pkg.dev/vaarahigcpproject/cloud-run-source-deploy/clinical-data-generator:incremental-v2 --region=us-central1 --project=vaarahigcpproject
