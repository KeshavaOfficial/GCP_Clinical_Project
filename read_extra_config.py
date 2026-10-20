from google.cloud import bigquery
from airflow.exceptions import AirflowException

PROJECT_ID = "vaarahigcpproject"
PIPELINE_ID = "CLINICAL_API_001"


def read_extra_config():
    client = bigquery.Client(project=PROJECT_ID)

    sql = """
    SELECT
        column_name,
        target_datatype,
        is_required,
        validation_rule,
        is_dedup_key
    FROM `vaarahigcpproject.clinical_control.extra_config`
    WHERE pipeline_id = @pipeline_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "pipeline_id", "STRING", PIPELINE_ID
            )
        ]
    )

    rows = list(client.query(sql, job_config=job_config).result())

    if not rows:
        raise AirflowException("No extra_config records found")

    rules = [dict(row.items()) for row in rows]

    print(f"Loaded {len(rules)} transformation rules")

    return rules