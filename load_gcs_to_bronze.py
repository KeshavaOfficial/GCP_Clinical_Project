
import hashlib
from google.cloud import bigquery


PROJECT_ID = "vaarahigcpproject"


def load_gcs_to_bronze(**context):

    ti = context["ti"]

    config = ti.xcom_pull(
        task_ids="read_pipeline_config"
    )

    gcs_uri = ti.xcom_pull(
        task_ids="check_gcs_file"
    )

    if not gcs_uri:
        raise ValueError("No verified GCS file found")

    if config["source_format"].upper() != "NEWLINE_DELIMITED_JSON":
        raise ValueError("Unsupported source format")

    client = bigquery.Client(project=PROJECT_ID)

    table_id = config["bronze_table"]

    # Unique job ID for each source file and destination
    job_key = f"{table_id}|{gcs_uri}"
    job_hash = hashlib.sha256(
        job_key.encode("utf-8")
    ).hexdigest()[:32]

    job_id = f"clinical_api_load_{job_hash}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
    )

    # Reuse an existing job when Airflow retries
    try:
        job = client.get_job(job_id, location="US")
        print("Existing BigQuery job found:", job_id)

    except Exception as error:
        from google.api_core.exceptions import NotFound

        if not isinstance(error, NotFound):
            raise

        try:
            job = client.load_table_from_uri(
                gcs_uri,
                table_id,
                job_config=job_config,
                job_id=job_id,
                location="US",
            )
        except Exception as submit_error:
            from google.api_core.exceptions import Conflict

            if not isinstance(submit_error, Conflict):
                raise

            job = client.get_job(job_id, location="US")

    # Wait until the load finishes
    job.result()

    print("Bronze load successful")
    print("Source:", gcs_uri)
    print("Destination:", table_id)
    print("BigQuery Job ID:", job_id)
    print("Rows loaded:", job.output_rows)

    return {
        "source_file": gcs_uri,
        "bronze_table": table_id,
        "job_id": job_id,
        "rows_loaded": job.output_rows,
    }
