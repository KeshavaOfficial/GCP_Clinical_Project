
from google.cloud import storage


def check_gcs_file(**context):

    ti = context["ti"]

    config = ti.xcom_pull(
        task_ids="read_pipeline_config"
    )

    file_path = ti.xcom_pull(
        task_ids="trigger_flask_api"
    )

    if not file_path:
        raise ValueError("Flask API did not return a file path")

    bucket_name = config["source_bucket"]

    client = storage.Client(project="vaarahigcpproject")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    if not blob.exists():
        raise FileNotFoundError(
            f"File not found: gs://{bucket_name}/{file_path}"
        )

    gcs_uri = f"gs://{bucket_name}/{file_path}"

    print("Verified GCS file:", gcs_uri)

    return gcs_uri
