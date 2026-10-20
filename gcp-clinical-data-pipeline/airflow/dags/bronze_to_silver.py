"""Incrementally synchronize 12 clinical Bronze tables into typed Silver tables.

Run via Airflow PythonOperator after successful Bronze live MERGE.
Requires google-cloud-bigquery and merge_clinical_incremental.KEYS.
"""

import os
from google.cloud import bigquery
from merge_clinical_incremental import KEYS

PROJECT = os.environ["GCP_PROJECT_ID"]
BRONZE_DATASET = os.environ.get("BRONZE_DATASET", "paraxel_bronze")
SILVER_DATASET = os.environ.get("SILVER_DATASET", "paraxel_silver")


def _quoted(name):
    if not name.replace("_", "").isalnum() or not name or name[0].isdigit():
        raise ValueError(f"Unsafe identifier: {name!r}")
    return f"`{name}`"


def _clean_expression(column, datatype):
    raw = f"NULLIF(TRIM(CAST(B.{_quoted(column)} AS STRING)), '')"
    if datatype.upper() == "STRING":
        return raw
    # BigQuery table schema types can differ from valid CAST target names.
    cast_types = {"FLOAT": "FLOAT64", "INTEGER": "INT64", "BOOLEAN": "BOOL",
                  "RECORD": "STRUCT"}
    target_type = cast_types.get(datatype.upper(), datatype.upper())
    # SAFE_CAST turns malformed values into NULL instead of failing the job.
    return f"SAFE_CAST({raw} AS {target_type})"


def process_bronze_to_silver(**_context):
    """Synchronize each table. Retry-safe for stable Bronze input and unique keys."""
    client = bigquery.Client(project=PROJECT)
    summary = {}

    for table, key in KEYS.items():
        bronze_id = f"{PROJECT}.{BRONZE_DATASET}.{table}"
        silver_id = f"{PROJECT}.{SILVER_DATASET}.{table}"
        bronze = client.get_table(bronze_id)
        silver = client.get_table(silver_id)

        bronze_columns = {field.name for field in bronze.schema}
        silver_fields = list(silver.schema)
        columns = [field.name for field in silver_fields]
        if key not in columns or key not in bronze_columns:
            raise ValueError(f"Primary key {key!r} missing from {table}")
        missing = set(columns) - bronze_columns
        if missing:
            raise ValueError(f"Missing Bronze columns in {table}: {sorted(missing)}")
        if any(field.field_type.upper() in {"RECORD", "STRUCT", "ARRAY"} or field.mode == "REPEATED"
               for field in silver_fields):
            raise ValueError(f"Nested/repeated Silver fields need explicit mapping: {table}")

        projections = ",\n        ".join(
            f"{_clean_expression(field.name, field.field_type)} AS {_quoted(field.name)}"
            for field in silver_fields
        )
        # Stable deterministic tie-break for duplicate keys. A source update-time
        # field should replace this tie-break if business rules require latest-wins.
        source = f"""(
          SELECT * EXCEPT(_rn) FROM (
            SELECT C.*, ROW_NUMBER() OVER (
              PARTITION BY {_quoted(key)}
              ORDER BY TO_JSON_STRING(C) DESC
            ) AS _rn
            FROM (
              SELECT {projections}
              FROM `{bronze_id}` AS B
            ) AS C
            WHERE {_quoted(key)} IS NOT NULL
          ) WHERE _rn = 1
        )"""
        updates = [col for col in columns if col != key]
        set_clause = ",\n    ".join(
            f"T.{_quoted(col)} = S.{_quoted(col)}" for col in updates
        )
        distinct_check = " OR ".join(
            f"T.{_quoted(col)} IS DISTINCT FROM S.{_quoted(col)}" for col in updates
        )
        insert_columns = ", ".join(_quoted(col) for col in columns)
        insert_values = ", ".join(f"S.{_quoted(col)}" for col in columns)
        sql = f"""
        MERGE `{silver_id}` AS T
        USING {source} AS S
        ON T.{_quoted(key)} = S.{_quoted(key)}
        WHEN MATCHED AND ({distinct_check}) THEN
          UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns}) VALUES ({insert_values})
        """
        job = client.query(sql)
        job.result()
        summary[table] = {"affected_rows": job.num_dml_affected_rows or 0}
        print(f"SILVER MERGE SUCCESS: {table}, affected_rows={summary[table]['affected_rows']}", flush=True)

    return summary
