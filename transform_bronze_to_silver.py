
import re
import uuid

from google.cloud import bigquery
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context


PROJECT_ID = "vaarahigcpproject"
PIPELINE_ID = "CLINICAL_API_001"
TEMP_DATASET = "paraxel_bronze"

COLUMNS = [
    "event_id",
    "patient_id",
    "study_id",
    "site_id",
    "enrollment_id",
    "age",
    "condition",
    "treatment_arm",
    "event_type",
    "event_status",
    "event_timestamp",
    "source_system",
]

STRING_COLUMNS = [
    col for col in COLUMNS
    if col not in ("age", "event_timestamp")
]

ALLOWED_TYPES = {
    **{col: "STRING" for col in STRING_COLUMNS},
    "age": "NUMERIC",
    "event_timestamp": "TIMESTAMP",
}

ALLOWED_RULES = {
    "NOT_NULL",
    "AGE_RANGE_0_120",
    "VALID_TIMESTAMP",
    "VALID_STATUS",
}

TABLE_PATTERN = re.compile(
    r"[A-Za-z0-9_-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+"
)


def transform_bronze_to_silver():

    context = get_current_context()
    ti = context["ti"]

    client = bigquery.Client(project=PROJECT_ID)

    config = ti.xcom_pull(
        task_ids="read_pipeline_config"
    )

    rules = ti.xcom_pull(
        task_ids="read_extra_config"
    )

    gcs_file = ti.xcom_pull(
        task_ids="trigger_flask_api"
    )

    # ---------------------------------------
    # 1. Validate pipeline configuration
    # ---------------------------------------

    if not isinstance(config, dict):
        raise AirflowException(
            "Pipeline configuration must be a dictionary"
        )

    if not isinstance(rules, list) or not rules:
        raise AirflowException(
            "No extra_config transformation rules found"
        )

    if not isinstance(gcs_file, str):
        raise AirflowException(
            "Flask task did not return a GCS file path"
        )

    source_bucket = config["source_bucket"]
    source_path = config["source_path"].strip("/")

    expected_prefix = f"gs://{source_bucket}/{source_path}/"

    # Flask may return either a GCS URI or an object path.
    if gcs_file.startswith("gs://"):
        full_gcs_uri = gcs_file
    else:
        full_gcs_uri = f"gs://{source_bucket}/{gcs_file.lstrip('/')}"

    # Validate the bucket and configured folder.
    if not full_gcs_uri.startswith(expected_prefix):
        raise AirflowException(
            f"GCS file is outside configured source: {gcs_file}"
        )

    # Use the complete URI for subsequent BigQuery loading.
    gcs_file = full_gcs_uri

    if config.get("pipeline_id") != PIPELINE_ID:
        raise AirflowException(
            "Unexpected pipeline configuration"
        )

    if config.get("source_format") != "NEWLINE_DELIMITED_JSON":
        raise AirflowException(
            "Unsupported source format"
        )

    if config.get("load_type", "").upper() != "INCREMENTAL":
        raise AirflowException(
            "Only INCREMENTAL load is supported"
        )

    bronze_table = config["bronze_table"]
    silver_table = config["silver_table"]

    for table_id in (bronze_table, silver_table):
        if not TABLE_PATTERN.fullmatch(table_id):
            raise AirflowException(
                f"Invalid BigQuery table identifier: {table_id}"
            )

    if silver_table != (
        "vaarahigcpproject.paraxel_silver.clinical_json"
    ):
        raise AirflowException(
            "Unexpected Silver destination"
        )

    # ---------------------------------------
    # 2. Validate extra_config rules
    # ---------------------------------------

    configured = {}

    for rule in rules:

        col = rule["column_name"]

        if col in configured:
            raise AirflowException(
                f"Duplicate configuration for {col}"
            )

        if col not in COLUMNS:
            raise AirflowException(
                f"Unknown column: {col}"
            )

        datatype = rule["target_datatype"].upper()

        if datatype != ALLOWED_TYPES[col]:
            raise AirflowException(
                f"Unsupported datatype for {col}: {datatype}"
            )

        validation = rule["validation_rule"]

        if validation not in ALLOWED_RULES:
            raise AirflowException(
                f"Unsupported validation rule: {validation}"
            )

        configured[col] = rule

    dedup_keys = [
        col
        for col, rule in configured.items()
        if rule["is_dedup_key"]
    ]

    if dedup_keys != ["event_id"]:
        raise AirflowException(
            "event_id must be the only deduplication key"
        )

    # ---------------------------------------
    # 3. Prepare batch staging table
    # ---------------------------------------

    temp_table = (
        f"{PROJECT_ID}.{TEMP_DATASET}."
        f"silver_batch_{uuid.uuid4().hex[:20]}"
    )

    # All incoming fields are loaded as STRING.
    # Conversion happens using SAFE_CAST.

    load_config = bigquery.LoadJobConfig(
        source_format=(
            bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        ),
        schema=[
            bigquery.SchemaField(
                col,
                "INTEGER" if col == "age" else "STRING"
            )
            for col in COLUMNS
        ],
        
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
    )

    # ---------------------------------------
    # 4. Build normalization expressions
    # ---------------------------------------

    normalized_columns = []

    for col in COLUMNS:

        raw_value = (
            f"NULLIF(TRIM(CAST(`{col}` AS STRING)), '')"
        )

        if col == "age":
            expression = (
                f"SAFE_CAST({raw_value} AS NUMERIC) AS `age`"
            )

        elif col == "event_timestamp":
            expression = (
                f"SAFE_CAST({raw_value} AS TIMESTAMP) "
                "AS `event_timestamp`"
            )

        else:
            expression = f"{raw_value} AS `{col}`"

        normalized_columns.append(expression)

    normalized_sql = ",\n".join(normalized_columns)

    # ---------------------------------------
    # 5. Build validation conditions
    # ---------------------------------------

    conditions = []

    for col, rule in configured.items():

        validation = rule["validation_rule"]

        if rule["is_required"] or validation == "NOT_NULL":
            conditions.append(
                f"`{col}` IS NOT NULL"
            )

        if validation == "AGE_RANGE_0_120":
            conditions.append(
                "(`age` IS NULL OR "
                "`age` BETWEEN 0 AND 120)"
            )

        elif validation == "VALID_TIMESTAMP":
            conditions.append(
                "`event_timestamp` IS NOT NULL"
            )

        elif validation == "VALID_STATUS":
            # Interim rule:
            # NULL allowed for optional status.
            # Non-NULL status must not be blank.
            # Define an approved status list later.
            conditions.append(
                "(`event_status` IS NULL "
                "OR `event_status` != '')"
            )

    valid_condition = (
        " AND ".join(conditions)
        if conditions
        else "TRUE"
    )

    # ---------------------------------------
    # 6. Prepare MERGE statements
    # ---------------------------------------

    selected_columns = ", ".join(
        f"`{col}`" for col in COLUMNS
    )

    source_columns = ", ".join(
        f"S.`{col}`" for col in COLUMNS
    )

    update_columns = ",\n".join(
        f"T.`{col}` = S.`{col}`"
        for col in COLUMNS
        if col != "event_id"
    )

    # ---------------------------------------
    # 7. Load and transform current batch
    # ---------------------------------------

    try:

        print("Current batch file:", gcs_file)
        print("Bronze table:", bronze_table)
        print("Silver table:", silver_table)

        load_job = client.load_table_from_uri(
            gcs_file,
            temp_table,
            job_config=load_config,
            location="US",
        )

        load_job.result()

        print("Current batch loaded to staging")

        sql = f"""
        CREATE TEMP TABLE normalized AS
        SELECT
            {normalized_sql}
        FROM `{temp_table}`;

        CREATE TEMP TABLE validated AS
        SELECT
            *,
            ({valid_condition}) AS is_valid
        FROM normalized;

        CREATE TEMP TABLE accepted AS
        SELECT * EXCEPT(is_valid)
        FROM validated
        WHERE is_valid
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY event_timestamp DESC NULLS LAST
        ) = 1;

        MERGE `{silver_table}` AS T
        USING accepted AS S
        ON T.event_id = S.event_id

        WHEN MATCHED THEN
            UPDATE SET
                {update_columns}

        WHEN NOT MATCHED THEN
            INSERT ({selected_columns})
            VALUES ({source_columns});

        SELECT
            (SELECT COUNT(*) FROM normalized)
                AS source_count,

            (SELECT COUNT(*) FROM accepted)
                AS success_count,

            (
                (SELECT COUNT(*) FROM normalized)
                -
                (SELECT COUNT(*) FROM accepted)
            ) AS rejected_count;
        """

        query_job = client.query(
            sql,
            location="US"
        )

        query_job.result()

        # Retrieve the final SELECT from the
        # multi-statement BigQuery script.

        child_jobs = list(
            client.list_jobs(
                parent_job=query_job.job_id
            )
        )

        select_jobs = [
            job for job in child_jobs
            if job.statement_type == "SELECT"
        ]

        if not select_jobs:
            raise AirflowException(
                "Transformation counts SELECT not found"
            )

        final_select = client.get_job(
            select_jobs[-1].job_id,
            location="US"
        )

        result = list(final_select.result())

        if not result:
            raise AirflowException(
                "Transformation returned no counts"
            )

        counts = dict(result[0].items())

        print("Silver transformation completed")
        print("Source records:", counts["source_count"])
        print("Accepted records:", counts["success_count"])
        print("Rejected records:", counts["rejected_count"])

        return counts

    finally:

        client.delete_table(
            temp_table,
            not_found_ok=True
        )

        print("Temporary staging table cleanup attempted")
