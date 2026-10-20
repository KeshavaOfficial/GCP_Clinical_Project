from google.cloud import bigquery
from airflow.operators.python import get_current_context

PROJECT_ID = "vaarahigcpproject"
AUDIT_TABLE = f"{PROJECT_ID}.clinical_control.pipeline_audit"
PIPELINE_ID = "CLINICAL_API_001"


def audit_success():
    context = get_current_context()
    ti = context["ti"]
    run_id = context["run_id"]

    counts = ti.xcom_pull(
        task_ids="transform_bronze_to_silver"
    )

    if not isinstance(counts, dict):
        raise ValueError("Missing Silver transformation counts")

    sql = f"""
    UPDATE `{AUDIT_TABLE}`
    SET
        end_time = CURRENT_TIMESTAMP(),
        source_count = @source_count,
        success_count = @success_count,
        rejected_count = @rejected_count,
        status = 'SUCCESS',
        error_message = NULL
    WHERE run_id = @run_id
      AND pipeline_id = @pipeline_id
    """

    parameters = [
        bigquery.ScalarQueryParameter(
            "run_id", "STRING", run_id
        ),
        bigquery.ScalarQueryParameter(
            "pipeline_id", "STRING", PIPELINE_ID
        ),
        bigquery.ScalarQueryParameter(
            "source_count", "INT64", counts["source_count"]
        ),
        bigquery.ScalarQueryParameter(
            "success_count", "INT64", counts["success_count"]
        ),
        bigquery.ScalarQueryParameter(
            "rejected_count", "INT64", counts["rejected_count"]
        )
    ]

    client = bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.QueryJobConfig(
        query_parameters=parameters
    )

    job = client.query(sql, job_config=job_config)
    job.result()

    if job.num_dml_affected_rows != 1:
        raise RuntimeError(
            "Expected exactly one audit row to update"
        )

    print(f"Audit SUCCESS updated for {run_id}")