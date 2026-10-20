"""Schema-aware incremental MERGE for 12 clinical Bronze tables.



SAFE DEFAULT: dry_run=True. Runs read-only preflight queries and BigQuery SQL dry runs.

Set dry_run=False only after inspecting the preflight output and backup.



This updates existing business columns and inserts new rows. Existing metadata is

preserved on UPDATE; new patients rows get batch_id, source_file, timestamp.

"""

import re

from google.cloud import bigquery



PROJECT = 'vaarahigcpproject'

DATASET = 'paraxel_bronze'

ARCHIVE = 'paraxel_bronze_archive'

# Live backups are always batch-specific; no global fallback.

AUDIT_TABLE = f'{PROJECT}.clinical_control.incremental_merge_audit'

META = {'batch_id', 'source_file', 'ingestion_timestamp'}

KEYS = {

    'adverse_events': 'ae_id', 'clinical_sites': 'site_id',

    'clinical_studies': 'study_id', 'investigators': 'investigator_id',

    'lab_results': 'lab_result_id', 'medical_history': 'history_id',

    'medications': 'medication_id', 'patient_enrollments': 'enrollment_id',

    'patient_visits': 'visit_id', 'patients': 'patient_id',

    'study_treatments': 'treatment_id', 'vital_signs': 'vital_id',

}

CAST_TYPES = {'DATE', 'DATETIME', 'TIMESTAMP', 'TIME', 'INT64', 'FLOAT64',

              'NUMERIC', 'BIGNUMERIC', 'BOOL'}





def q(name):

    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', name):

        raise ValueError(f'Invalid identifier: {name}')

    return f'`{name}`'





def source_expr(field):

    col = f'S.{q(field.name)}'

    typ = field.field_type.upper()

    if typ == 'STRING':

        return col

    if typ in CAST_TYPES:

        return f"SAFE_CAST(NULLIF(TRIM({col}), '') AS {typ})"

    raise ValueError(f'Unsupported column type: {field.name} {typ}')





def audit_write(client, batch_id, name, status, counts=None, error=None):

    """One audit row per batch/table. Preserve initial counts across retries."""

    counts = counts or {}

    sql = f"""MERGE `{AUDIT_TABLE}` T

      USING (SELECT @batch_id AS batch_id, @table_name AS table_name) S

      ON T.batch_id = S.batch_id AND T.table_name = S.table_name

      WHEN MATCHED THEN UPDATE SET

        new_records = COALESCE(T.new_records, @new_records),

        changed_records = COALESCE(T.changed_records, @changed_records),

        unchanged_records = COALESCE(T.unchanged_records, @unchanged_records),

        status = CASE WHEN T.status = 'SUCCESS' THEN 'SUCCESS' ELSE @status END,

        execution_timestamp = CURRENT_TIMESTAMP(),

        error_message = CASE WHEN T.status = 'SUCCESS' OR @status = 'SUCCESS'

                             THEN NULL ELSE @error END

      WHEN NOT MATCHED THEN INSERT

        (batch_id, table_name, new_records, changed_records,

         unchanged_records, status, execution_timestamp, error_message)

      VALUES (@batch_id, @table_name, @new_records, @changed_records,

              @unchanged_records, @status, CURRENT_TIMESTAMP(), @error)"""

    config = bigquery.QueryJobConfig(query_parameters=[

        bigquery.ScalarQueryParameter('batch_id', 'STRING', batch_id),

        bigquery.ScalarQueryParameter('table_name', 'STRING', name),

        bigquery.ScalarQueryParameter('status', 'STRING', status),

        bigquery.ScalarQueryParameter('new_records', 'INT64', counts.get('new_records')),

        bigquery.ScalarQueryParameter('changed_records', 'INT64', counts.get('changed_records')),

        bigquery.ScalarQueryParameter('unchanged_records', 'INT64', counts.get('unchanged_records')),

        bigquery.ScalarQueryParameter('error', 'STRING', (str(error)[:2000] if error else None)),

    ])

    client.query(sql, job_config=config).result()

    print(f'AUDIT {name}: {status}', flush=True)





def one(client, name, key, batch_id, dry_run, audit_before_merge=None, backup_suffix=None):

    target_id = f'{PROJECT}.{DATASET}.{name}'

    stage_id = f'{target_id}_staging'

    backup_id = f'{PROJECT}.{ARCHIVE}.{name}{backup_suffix}' if backup_suffix else None

    target = client.get_table(target_id)

    stage = client.get_table(stage_id)

    if not dry_run:
        if not backup_id:
            raise ValueError('Live MERGE requires a batch-specific backup')
        client.get_table(backup_id)  # fail closed when missing
    business = [f for f in target.schema if f.name not in META]

    stage_names = {f.name for f in stage.schema}

    missing = [f.name for f in business if f.name not in stage_names]

    if missing or key not in stage_names:

        raise ValueError(f'{name}: staging missing columns {missing}, key={key}')

    if not dry_run:
        # Backups are immutable. Allow partial-retry Bronze growth, but never missing keys.
        backup_rows = next(iter(client.query(
            f'SELECT COUNT(*) AS n FROM `{backup_id}`').result())).n
        target_rows = next(iter(client.query(
            f'SELECT COUNT(*) AS n FROM `{target_id}`').result())).n
        if target_rows < backup_rows:
            raise ValueError(f'{name}: Bronze rows {target_rows} below backup rows {backup_rows}')
        missing_backup = next(iter(client.query(
            f'SELECT COUNT(*) AS n FROM `{backup_id}` B '
            f'WHERE NOT EXISTS (SELECT 1 FROM `{target_id}` T WHERE T.{q(key)} = B.{q(key)})'
        ).result())).n
        if missing_backup:
            raise ValueError(f'{name}: {missing_backup} backup keys missing from Bronze')
    else:
        target_rows = None

    if not business or key not in {f.name for f in business}:

        raise ValueError(f'{name}: invalid business schema or key')

    # Ensure staging matches intended batch. Staging is shared, so other DAG

    # runs must be stopped while this function executes.

    prefix = f'clinical_source/{batch_id}/{name}.csv'

    checks = [f"COUNTIF(S.{q(key)} IS NULL OR TRIM(S.{q(key)}) = '') AS null_keys",

              f'COUNT(*) - COUNT(DISTINCT S.{q(key)}) AS duplicate_keys']

    for f in business:

        if f.field_type.upper() != 'STRING':

            expr = source_expr(f)

            checks.append(f"COUNTIF(NULLIF(TRIM(S.{q(f.name)}), '') IS NOT NULL AND {expr} IS NULL) AS bad_{f.name}")

    check_sql = f'SELECT {", ".join(checks)} FROM `{stage_id}` S'

    result = dict(next(iter(client.query(check_sql).result())).items())

    if any(result.values()):

        raise ValueError(f'{name}: staging validation failed: {result}')

    old_check = f'''SELECT COUNT(*) - COUNT(DISTINCT {q(key)}) AS duplicates,

        COUNTIF({q(key)} IS NULL OR TRIM(CAST({q(key)} AS STRING)) = '') AS missing

        FROM `{target_id}`'''

    old = next(iter(client.query(old_check).result()))

    if old.duplicates or old.missing:

        raise ValueError(f'{name}: Bronze duplicate or missing keys')

    fields = [f.name for f in business]

    incoming = ', '.join(f'{source_expr(f)} AS {q(f.name)}' for f in business)

    left = ', '.join(f'I.{q(c)} AS {q(c)}' for c in fields)

    right = ', '.join(f'T.{q(c)} AS {q(c)}' for c in fields)

    # Use exact comparison semantics of compare_clinical_incremental.py.

    changed = f'TO_JSON_STRING(STRUCT({left})) != TO_JSON_STRING(STRUCT({right}))'

    # Count changes before writing, to support post-MERGE verification.

    compare_sql = f'''WITH I AS (SELECT {incoming} FROM `{stage_id}` S)

      SELECT COUNTIF(T.{q(key)} IS NULL) AS new_records,

        COUNTIF(T.{q(key)} IS NOT NULL AND {changed}) AS changed_records,

        COUNTIF(T.{q(key)} IS NOT NULL AND NOT ({changed})) AS unchanged_records

      FROM I LEFT JOIN `{target_id}` T ON I.{q(key)} = T.{q(key)}'''

    counts = dict(next(iter(client.query(compare_sql).result())).items())

    print(f'PREFLIGHT {name}: {counts}', flush=True)

    update_fields = [c for c in fields if c != key]

    if not update_fields:

        raise ValueError(f'{name}: no update fields')

    update_set = ', '.join(f'{q(c)} = I.{q(c)}' for c in update_fields)

    insert_cols = fields[:]

    insert_values = [f'I.{q(c)}' for c in fields]

    target_meta = {f.name: f for f in target.schema if f.name in META}

    if 'batch_id' in target_meta:

        insert_cols.append('batch_id')

        insert_values.append('@batch_id')

    if 'source_file' in target_meta:

        insert_cols.append('source_file')

        insert_values.append('@source_file')

    if 'ingestion_timestamp' in target_meta:

        if target_meta['ingestion_timestamp'].field_type.upper() != 'TIMESTAMP':

            raise ValueError(f'{name}: unexpected ingestion_timestamp type')

        insert_cols.append('ingestion_timestamp')

        insert_values.append('CURRENT_TIMESTAMP()')

    merge_sql = f'''MERGE `{target_id}` T

      USING (SELECT {incoming} FROM `{stage_id}` S) I

      ON T.{q(key)} = I.{q(key)}

      WHEN MATCHED AND {changed} THEN UPDATE SET {update_set}

      WHEN NOT MATCHED THEN INSERT ({', '.join(q(c) for c in insert_cols)})

      VALUES ({', '.join(insert_values)})'''

    config = bigquery.QueryJobConfig(query_parameters=[

        bigquery.ScalarQueryParameter('batch_id', 'STRING', batch_id),

        bigquery.ScalarQueryParameter('source_file', 'STRING',

                                     f'gs://personalproject_1/{prefix}')

    ])

    if dry_run:

        config.dry_run = True

        config.use_query_cache = False

        client.query(merge_sql, job_config=config)

        print(f'DRY RUN OK {name}', flush=True)

    else:

        if audit_before_merge is not None:

            audit_before_merge(counts)  # durable original counts before the write

        job = client.query(merge_sql, job_config=config, job_id_prefix=f'clinical_merge_{name}_')

        job.result()

        print(f'MERGED {name}: affected_rows={job.num_dml_affected_rows}', flush=True)

        # Rerun read-only comparison; must be zero new/changed.

        after = dict(next(iter(client.query(compare_sql).result())).items())

        if after['new_records'] or after['changed_records']:

            raise RuntimeError(f'{name}: post-MERGE differences remain: {after}')

        expected = target_rows + counts['new_records']

        # Verify exact row count after MERGE.

        actual = next(iter(client.query(f'SELECT COUNT(*) AS n FROM `{target_id}`').result())).n

        if actual != expected:

            raise RuntimeError(f'{name}: expected {expected} rows, got {actual}')

        print(f'POSTCHECK {name}: rows={actual}, remaining_changes=0', flush=True)

    return counts





def merge_clinical_incremental(batch_id: str, dry_run: bool = True, backup_suffix=None, **_context):

    if not re.fullmatch(r'[A-Za-z0-9_-]+', batch_id):

        raise ValueError(f'Unsafe batch_id: {batch_id!r}')

    if not dry_run and not backup_suffix:

        raise ValueError('Live MERGE requires a verified batch-specific backup_suffix')

    if backup_suffix and not re.fullmatch(r'_premerge_[A-Za-z0-9_]+', backup_suffix):

        raise ValueError('Unsafe backup suffix')

    client = bigquery.Client(project=PROJECT)

    result = {}

    for name, key in KEYS.items():

        if dry_run:

            result[name] = one(client, name, key, batch_id, True)

            continue

        # Completed tables must never be applied again for the same batch.
        audit_sql = f'''SELECT new_records, changed_records, unchanged_records
            FROM `{AUDIT_TABLE}`
            WHERE batch_id = @batch_id AND table_name = @table_name
              AND status = 'SUCCESS'
            ORDER BY execution_timestamp DESC LIMIT 1'''
        audit_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter('batch_id', 'STRING', batch_id),
            bigquery.ScalarQueryParameter('table_name', 'STRING', name),
        ])
        completed = list(client.query(audit_sql, job_config=audit_config).result())
        if completed:
            counts = {k: int(completed[0][k] or 0)
                      for k in ('new_records', 'changed_records', 'unchanged_records')}
            print(f'ALREADY SUCCESS {name} batch={batch_id}; skipping MERGE', flush=True)
            result[name] = counts
            continue

        original_counts = None

        def mark_started(counts):

            nonlocal original_counts

            original_counts = counts

            audit_write(client, batch_id, name, 'STARTED', counts)

        try:

            counts = one(client, name, key, batch_id, False,

                         audit_before_merge=mark_started, backup_suffix=backup_suffix)

            audit_write(client, batch_id, name, 'SUCCESS', counts)

            result[name] = counts

        except Exception as exc:

            try:

                audit_write(client, batch_id, name, 'FAILED', original_counts, exc)

            except Exception as audit_exc:

                print(f'AUDIT FAILURE {name}: {audit_exc!r}', flush=True)

            raise

    totals = {k: sum(v[k] for v in result.values())

              for k in ('new_records', 'changed_records', 'unchanged_records')}

    print(f'COMPLETE dry_run={dry_run} batch={batch_id} totals={totals}', flush=True)

    return totals
