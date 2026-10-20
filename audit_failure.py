from google.cloud import bigquery

PROJECT_ID = "vaarahigcpproject"
PIPELINE_ID = "CLINICAL_API_001"
AUDIT_TABLE = f"{PROJECT_ID}.clinical_control.pipeline_audit"


def audit_failure(context):
    run_id = context["run_id"]
    task = context.get("task_instance")

    task_id = task.task_id if task else "unknown"
    exception = context.get("exception")

    error_message = (
        f"Task: {task_id}; Error: {str(exception)}"
    )[:2000]

    client = bigquery.Client(project=PROJECT_ID)

    sql = f"""
    MERGE `{AUDIT_TABLE}` T
    USING (
        SELECT
            @run_id AS run_id,
            @pipeline_id AS pipeline_id,
            @error_message AS error_message
    ) S
    ON T.run_id = S.run_id
       AND T.pipeline_id = S.pipeline_id

    WHEN MATCHED THEN
      UPDATE SET
        end_time = CURRENT_TIMESTAMP(),
        status = 'FAILED',
        error_message = S.error_message

    WHEN NOT MATCHED THEN
      INSERT (
        run_id,
        pipeline_id,
        start_time,
        end_time,
        status,
        error_message
      )
      VALUES (
        S.run_id,
        S.pipeline_id,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        'FAILED',
        S.error_message
      )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "run_id", "STRING", run_id
            ),
            bigquery.ScalarQueryParameter(
                "pipeline_id", "STRING", PIPELINE_ID
            ),
            bigquery.ScalarQueryParameter(
                "error_message", "STRING", error_message
            )
        ]
    )

    client.query(sql, job_config=job_config).result()