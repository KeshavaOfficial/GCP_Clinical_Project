from airflow import DAG

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from google.cloud import storage
from google.cloud import bigquery

from datetime import datetime, timedelta

import config


# ==========================================================
# GCP CLIENT FUNCTIONS
# ==========================================================

def create_bucket():

    client = storage.Client(project=config.PROJECT_ID)

    bucket = client.lookup_bucket(config.BUCKET_NAME)

    if bucket is None:

        bucket = client.bucket(config.BUCKET_NAME)

        bucket.location = config.REGION

        client.create_bucket(bucket)

        print("Bucket created:", config.BUCKET_NAME)

    else:

        print("Bucket already exists:", config.BUCKET_NAME)


# ==========================================================
# COPY LOCAL FILE TO GCS
# ==========================================================

def copy_file_to_bucket():

    client = storage.Client(project=config.PROJECT_ID)

    bucket = client.bucket(config.BUCKET_NAME)

    blob = bucket.blob(config.GCS_FILE_NAME)

    blob.upload_from_filename(
        config.LOCAL_FILE_PATH
    )

    print(
        "File uploaded to:",
        f"gs://{config.BUCKET_NAME}/{config.GCS_FILE_NAME}"
    )


# ==========================================================
# CREATE DATASET FUNCTION
# ==========================================================

def create_dataset(dataset_name):

    client = bigquery.Client(
        project=config.PROJECT_ID
    )

    dataset_id = (
        f"{config.PROJECT_ID}.{dataset_name}"
    )

    dataset = bigquery.Dataset(dataset_id)

    dataset.location = config.BQ_LOCATION

    client.create_dataset(
        dataset,
        exists_ok=True
    )

    print(
        "Dataset ready:",
        dataset_id
    )


# ==========================================================
# CREATE STAGING TABLE
# ==========================================================

def create_staging_table():

    client = bigquery.Client(
        project=config.PROJECT_ID
    )

    table_id = (
        f"{config.PROJECT_ID}."
        f"{config.STAGING_DATASET}."
        f"{config.STAGING_TABLE}"
    )

    schema = [

        bigquery.SchemaField(
            "patient_id",
            "STRING"
        ),

        bigquery.SchemaField(
            "study_id",
            "STRING"
        ),

        bigquery.SchemaField(
            "gender",
            "STRING"
        ),

        # STRING intentionally because source may contain
        # invalid values before cleaning.
        bigquery.SchemaField(
            "age",
            "STRING"
        ),

        bigquery.SchemaField(
            "country",
            "STRING"
        ),

        bigquery.SchemaField(
            "medical_condition",
            "STRING"
        ),

        bigquery.SchemaField(
            "treatment",
            "STRING"
        ),

        # Keep as STRING in staging because source
        # may contain INVALID_DATE.
        bigquery.SchemaField(
            "visit_date",
            "STRING"
        ),

        bigquery.SchemaField(
            "trial_phase",
            "STRING"
        ),

        bigquery.SchemaField(
            "study_status",
            "STRING"
        ),

        bigquery.SchemaField(
            "adverse_event",
            "STRING"
        )
    ]

    table = bigquery.Table(
        table_id,
        schema=schema
    )

    client.create_table(
        table,
        exists_ok=True
    )

    print(
        "Staging table ready:",
        table_id
    )


# ==========================================================
# CREATE AUDIT TABLE
# ==========================================================

def create_audit_table():

    client = bigquery.Client(
        project=config.PROJECT_ID
    )

    table_id = (
        f"{config.PROJECT_ID}."
        f"{config.AUDIT_DATASET}."
        f"{config.AUDIT_TABLE}"
    )

    schema = [

        bigquery.SchemaField(
            "dag_run_id",
            "STRING"
        ),

        bigquery.SchemaField(
            "file_name",
            "STRING"
        ),

        bigquery.SchemaField(
            "status",
            "STRING"
        ),

        bigquery.SchemaField(
            "start_time",
            "TIMESTAMP"
        ),

        bigquery.SchemaField(
            "end_time",
            "TIMESTAMP"
        ),

        bigquery.SchemaField(
            "source_count",
            "INTEGER"
        ),

        bigquery.SchemaField(
            "processed_count",
            "INTEGER"
        )
    ]

    table = bigquery.Table(
        table_id,
        schema=schema
    )

    client.create_table(
        table,
        exists_ok=True
    )

    print(
        "Audit table ready:",
        table_id
    )


# ==========================================================
# CREATE HISTORY TABLE
# ==========================================================

def create_history_table():

    client = bigquery.Client(
        project=config.PROJECT_ID
    )

    table_id = (
        f"{config.PROJECT_ID}."
        f"{config.HISTORY_DATASET}."
        f"{config.HISTORY_TABLE}"
    )

    schema = [

        bigquery.SchemaField(
            "patient_id",
            "STRING"
        ),

        bigquery.SchemaField(
            "study_id",
            "STRING"
        ),

        bigquery.SchemaField(
            "gender",
            "STRING"
        ),

        bigquery.SchemaField(
            "age",
            "INTEGER"
        ),

        bigquery.SchemaField(
            "country",
            "STRING"
        ),

        bigquery.SchemaField(
            "medical_condition",
            "STRING"
        ),

        bigquery.SchemaField(
            "treatment",
            "STRING"
        ),

        bigquery.SchemaField(
            "visit_date",
            "DATE"
        ),

        bigquery.SchemaField(
            "trial_phase",
            "STRING"
        ),

        bigquery.SchemaField(
            "study_status",
            "STRING"
        ),

        bigquery.SchemaField(
            "adverse_event",
            "STRING"
        ),

        bigquery.SchemaField(
            "load_timestamp",
            "TIMESTAMP"
        )
    ]

    table = bigquery.Table(
        table_id,
        schema=schema
    )

    client.create_table(
        table,
        exists_ok=True
    )

    print(
        "History table ready:",
        table_id
    )


# ==========================================================
# CREATE AUTH DATASET
# ==========================================================

def create_auth_dataset():

    create_dataset(
        config.AUTH_DATASET
    )


# ==========================================================
# INGEST GCS -> BIGQUERY STAGING
# ==========================================================

def ingest_gcs_to_staging():

    client = bigquery.Client(
        project=config.PROJECT_ID
    )

    table_id = (
        f"{config.PROJECT_ID}."
        f"{config.STAGING_DATASET}."
        f"{config.STAGING_TABLE}"
    )

    uri = (
        f"gs://{config.BUCKET_NAME}/"
        f"{config.GCS_FILE_NAME}"
    )

    job_config = bigquery.LoadJobConfig(

        source_format=bigquery.SourceFormat.CSV,

        skip_leading_rows=1,

        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
        )
    )

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )

    load_job.result()

    table = client.get_table(table_id)

    print(
        "Rows loaded to staging:",
        table.num_rows
    )


# ==========================================================
# INSERT AUDIT RECORD
# ==========================================================

def insert_audit_record(**context):

    client = bigquery.Client(
        project=config.PROJECT_ID
    )

    run_id = context["run_id"]

    query = f"""
    INSERT INTO
    `{config.PROJECT_ID}.{config.AUDIT_DATASET}.{config.AUDIT_TABLE}`

    (
        dag_run_id,
        file_name,
        status,
        start_time,
        end_time,
        source_count,
        processed_count
    )

    SELECT
        @run_id,
        @file_name,
        'STARTED',
        CURRENT_TIMESTAMP(),
        NULL,
        COUNT(*),
        NULL

    FROM
    `{config.PROJECT_ID}.{config.STAGING_DATASET}.{config.STAGING_TABLE}`
    """

    job_config = bigquery.QueryJobConfig(

        query_parameters=[

            bigquery.ScalarQueryParameter(
                "run_id",
                "STRING",
                run_id
            ),

            bigquery.ScalarQueryParameter(
                "file_name",
                "STRING",
                config.GCS_FILE_NAME
            )
        ]
    )

    client.query(
        query,
        job_config=job_config
    ).result()

    print(
        "Audit STARTED record inserted."
    )


# ==========================================================
# CLEAN + TRANSFORM + INSERT INTO HISTORY
#
# Removes:
# - exact duplicates
# - NULL values
# - blank values
# - whitespace values
#
# Also validates age and date.
# ==========================================================

def transform_and_insert_history():

    client = bigquery.Client(
        project=config.PROJECT_ID
    )

    query = f"""

    INSERT INTO
    `{config.PROJECT_ID}.{config.HISTORY_DATASET}.{config.HISTORY_TABLE}`

    (
        patient_id,
        study_id,
        gender,
        age,
        country,
        medical_condition,
        treatment,
        visit_date,
        trial_phase,
        study_status,
        adverse_event,
        load_timestamp
    )


    WITH cleaned_data AS
    (
        SELECT

            TRIM(patient_id)
                AS patient_id,

            TRIM(study_id)
                AS study_id,

            INITCAP(TRIM(gender))
                AS gender,

            SAFE_CAST(
                TRIM(age) AS INT64
            ) AS age,

            INITCAP(TRIM(country))
                AS country,

            TRIM(medical_condition)
                AS medical_condition,

            TRIM(treatment)
                AS treatment,

            SAFE_CAST(
                TRIM(visit_date) AS DATE
            ) AS visit_date,

            TRIM(trial_phase)
                AS trial_phase,

            TRIM(study_status)
                AS study_status,

            TRIM(adverse_event)
                AS adverse_event

        FROM
        `{config.PROJECT_ID}.{config.STAGING_DATASET}.{config.STAGING_TABLE}`

    ),


    valid_data AS
    (
        SELECT *

        FROM cleaned_data

        WHERE

            patient_id IS NOT NULL
            AND patient_id != ''

            AND study_id IS NOT NULL
            AND study_id != ''

            AND gender IS NOT NULL
            AND gender != ''

            AND age IS NOT NULL
            AND age BETWEEN 18 AND 100

            AND country IS NOT NULL
            AND country != ''

            AND medical_condition IS NOT NULL
            AND medical_condition != ''

            AND treatment IS NOT NULL
            AND treatment != ''

            AND visit_date IS NOT NULL

            AND trial_phase IS NOT NULL
            AND trial_phase != ''

            AND study_status IS NOT NULL
            AND study_status != ''

            AND adverse_event IS NOT NULL
            AND adverse_event != ''
    ),


    deduplicated_data AS
    (
        SELECT * EXCEPT(row_num)

        FROM
        (
            SELECT
                *,

                ROW_NUMBER() OVER
                (
                    PARTITION BY
                        patient_id,
                        study_id,
                        visit_date

                    ORDER BY
                        patient_id
                ) AS row_num

            FROM valid_data
        )

        WHERE row_num = 1
    )


    SELECT

        patient_id,
        study_id,
        gender,
        age,
        country,
        medical_condition,
        treatment,
        visit_date,
        trial_phase,
        study_status,
        adverse_event,

        CURRENT_TIMESTAMP()
            AS load_timestamp

    FROM deduplicated_data

    """

    client.query(query).result()

    print(
        "Cleaned data inserted into history."
    )


# ==========================================================
# UPDATE AUDIT TABLE
# ==========================================================

def update_audit_record(**context):

    client = bigquery.Client(
        project=config.PROJECT_ID
    )

    run_id = context["run_id"]

    query = f"""

    UPDATE
    `{config.PROJECT_ID}.{config.AUDIT_DATASET}.{config.AUDIT_TABLE}` A

    SET

        status = 'SUCCESS',

        end_time = CURRENT_TIMESTAMP(),

        processed_count =
        (
            SELECT COUNT(*)

            FROM
            `{config.PROJECT_ID}.{config.HISTORY_DATASET}.{config.HISTORY_TABLE}`

            WHERE DATE(load_timestamp) = CURRENT_DATE()
        )

    WHERE dag_run_id = @run_id

    """

    job_config = bigquery.QueryJobConfig(

        query_parameters=[

            bigquery.ScalarQueryParameter(
                "run_id",
                "STRING",
                run_id
            )
        ]
    )

    client.query(
        query,
        job_config=job_config
    ).result()

    print(
        "Audit record updated."
    )


# ==========================================================
# CREATE BUSINESS / AUTHORIZED VIEW
# ==========================================================

def create_business_view():

    client = bigquery.Client(
        project=config.PROJECT_ID
    )

    view_id = (
        f"{config.PROJECT_ID}."
        f"{config.AUTH_DATASET}."
        f"{config.AUTH_VIEW}"
    )

    view = bigquery.Table(view_id)

    view.view_query = f"""

    SELECT

        patient_id,
        study_id,
        gender,
        age,
        country,
        medical_condition,
        treatment,
        visit_date,
        trial_phase,
        study_status,
        adverse_event

    FROM
    `{config.PROJECT_ID}.{config.HISTORY_DATASET}.{config.HISTORY_TABLE}`

    """

    client.create_table(
        view,
        exists_ok=True
    )

    print(
        "Auth view ready:",
        view_id
    )


# ==========================================================
# DEFAULT ARGS
# ==========================================================

default_args = {

    "owner": "keshava",

    "depends_on_past": False,

    "retries": 2,

    "retry_delay": timedelta(minutes=5)
}


# ==========================================================
# DAG
# ==========================================================

with DAG(

    dag_id=config.DAG_ID,

    default_args=default_args,

    start_date=datetime(
        config.START_DATE_YEAR,
        config.START_DATE_MONTH,
        config.START_DATE_DAY
    ),

    schedule=config.SCHEDULE,

    catchup=False,

    description="Clinical GCS to BigQuery pipeline",

    tags=[
        "clinical",
        "gcs",
        "bigquery"
    ]

) as dag:


    # ------------------------------------------------------
    # START
    # ------------------------------------------------------

    start_task = EmptyOperator(
        task_id="start_task"
    )


    # ------------------------------------------------------
    # CREATE GCS BUCKET
    # ------------------------------------------------------

    create_buck = PythonOperator(

        task_id="create_bucket",

        python_callable=create_bucket
    )


    # ------------------------------------------------------
    # COPY LOCAL -> GCS
    # ------------------------------------------------------

    copy_to_buck = PythonOperator(

        task_id="copy_to_bucket",

        python_callable=copy_file_to_bucket
    )


    # ------------------------------------------------------
    # CREATE STAGING DATASET
    # ------------------------------------------------------

    create_staging_dataset = PythonOperator(

        task_id="create_staging_dataset",

        python_callable=create_dataset,

        op_args=[
            config.STAGING_DATASET
        ]
    )


    # ------------------------------------------------------
    # CREATE STAGING TABLE
    # ------------------------------------------------------

    create_staging_table_task = PythonOperator(

        task_id="create_staging_table",

        python_callable=create_staging_table
    )


    # ------------------------------------------------------
    # CREATE AUDIT DATASET
    # ------------------------------------------------------

    create_audit_dataset = PythonOperator(

        task_id="create_audit_dataset",

        python_callable=create_dataset,

        op_args=[
            config.AUDIT_DATASET
        ]
    )


    # ------------------------------------------------------
    # CREATE AUDIT TABLE
    # ------------------------------------------------------

    create_audit_table_task = PythonOperator(

        task_id="create_audit_table",

        python_callable=create_audit_table
    )


    # ------------------------------------------------------
    # CREATE HISTORY DATASET
    # ------------------------------------------------------

    create_history_dataset = PythonOperator(

        task_id="create_history_dataset",

        python_callable=create_dataset,

        op_args=[
            config.HISTORY_DATASET
        ]
    )


    # ------------------------------------------------------
    # CREATE HISTORY TABLE
    # ------------------------------------------------------

    create_history_table_task = PythonOperator(

        task_id="create_history_table",

        python_callable=create_history_table
    )


    # ------------------------------------------------------
    # CREATE AUTH DATASET
    # ------------------------------------------------------

    create_authview = PythonOperator(

        task_id="create_auth_dataset",

        python_callable=create_auth_dataset
    )


    # ------------------------------------------------------
    # GCS -> BIGQUERY STAGING
    # ------------------------------------------------------

    ingest_gcs_bqstaging = PythonOperator(

        task_id="ingest_gcs_to_bq_staging",

        python_callable=ingest_gcs_to_staging
    )


    # ------------------------------------------------------
    # AUDIT START
    # ------------------------------------------------------

    insert_audit_table = PythonOperator(

        task_id="insert_audit_table",

        python_callable=insert_audit_record
    )


    # ------------------------------------------------------
    # CLEAN + DEDUPLICATE + HISTORY LOAD
    # ------------------------------------------------------

    transform_data_insert_to_history = PythonOperator(

        task_id="transform_data_insert_to_history",

        python_callable=transform_and_insert_history
    )


    # ------------------------------------------------------
    # AUDIT SUCCESS
    # ------------------------------------------------------

    update_audit_table = PythonOperator(

        task_id="update_audit_table",

        python_callable=update_audit_record
    )


    # ------------------------------------------------------
    # CREATE AUTH VIEW
    # ------------------------------------------------------

    create_view = PythonOperator(

        task_id="create_view",

        python_callable=create_business_view
    )


    # ------------------------------------------------------
    # END
    # ------------------------------------------------------

    end_task = EmptyOperator(
        task_id="end_task"
    )


    # ======================================================
    # DEPENDENCIES
    # ======================================================

    (
        start_task

        >> create_buck

        >> copy_to_buck

        >> create_staging_dataset

        >> create_staging_table_task

        >> create_audit_dataset

        >> create_audit_table_task

        >> create_history_dataset

        >> create_history_table_task

        >> create_authview

        >> ingest_gcs_bqstaging

        >> insert_audit_table

        >> transform_data_insert_to_history

        >> update_audit_table

        >> create_view

        >> end_task
    )