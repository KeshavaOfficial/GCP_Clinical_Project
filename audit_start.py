from google.cloud import bigquery
from airflow.operators.python import get_current_context

PROJECT_ID = "vaarahigcpproject"
AUDIT_TABLE = f"{PROJECT_ID}.clinical_control.pipeline_audit"
PIPELINE_ID = "CLINICAL_API_001"


def audit_start():
    context = get_current_context()
    run_id = context["run_id"]

    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    MERGE `{AUDIT_TABLE}` AS target
    USING (
        SELECT
            @run_id AS run_id,
            @pipeline_id AS pipeline_id
    ) AS source
    ON target.run_id = source.run_id
       AND target.pipeline_id = source.pipeline_id

    WHEN NOT MATCHED THEN
      INSERT (
          run_id,
          pipeline_id,
          start_time,
          status
      )
      VALUES (
          source.run_id,
          source.pipeline_id,
          CURRENT_TIMESTAMP(),
          'RUNNING'
      )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("pipeline_id", "STRING", PIPELINE_ID),
        ]
    )

    client.query(query, job_config=job_config).result()
    print(f"Audit initialized for run: {run_id}")