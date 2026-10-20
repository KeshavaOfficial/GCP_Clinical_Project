from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)

def create_generator_task():
    return CloudRunExecuteJobOperator(
        task_id="trigger_clinical_generator",
        project_id="vaarahigcpproject",
        region="us-central1",
        job_name="clinical-data-generator",
        deferrable=False,
        gcp_conn_id="google_cloud_default",
    )