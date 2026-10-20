"""Read-only comparison of staged CSVs with 12 existing Bronze tables."""
from google.cloud import bigquery

PROJECT_ID = 'vaarahigcpproject'
DATASET = 'paraxel_bronze'
KEYS = {
    'adverse_events': 'ae_id', 'clinical_sites': 'site_id',
    'clinical_studies': 'study_id', 'investigators': 'investigator_id',
    'lab_results': 'lab_result_id', 'medical_history': 'history_id',
    'medications': 'medication_id', 'patient_enrollments': 'enrollment_id',
    'patient_visits': 'visit_id', 'patients': 'patient_id',
    'study_treatments': 'treatment_id', 'vital_signs': 'vital_id',
}
METADATA = {'batch_id', 'source_file', 'ingestion_timestamp'}


def _normalize(column, data_type):
    value = f's.`{column}`'
    typ = data_type.upper()
    if typ == 'STRING':
        return value
    if typ in ('DATE', 'DATETIME', 'TIMESTAMP', 'TIME', 'INT64', 'FLOAT64',
               'NUMERIC', 'BIGNUMERIC', 'BOOL'):
        return f"SAFE_CAST(NULLIF(TRIM({value}), '') AS {typ})"
    raise ValueError(f'Unsupported Bronze type for {column}: {data_type}')


def compare_clinical_incremental(**_context):
    client = bigquery.Client(project=PROJECT_ID)
    summary = {}
    for name, key in KEYS.items():
        target_id = f'{PROJECT_ID}.{DATASET}.{name}'
        stage_id = f'{target_id}_staging'
        target = client.get_table(target_id)
        stage = client.get_table(stage_id)
        target_fields = [f for f in target.schema if f.name not in METADATA]
        stage_columns = {f.name for f in stage.schema}
        missing = [f.name for f in target_fields if f.name not in stage_columns]
        if missing or key not in stage_columns:
            raise ValueError(f'{name}: staging columns missing: {missing}, key={key}')
        expressions = {f.name: _normalize(f.name, f.field_type) for f in target_fields}
        typed_select = ', '.join(f'{expr} AS `{col}`' for col, expr in expressions.items())
        fields = [f.name for f in target_fields]
        json_in = ', '.join(f'i.`{col}` AS `{col}`' for col in fields)
        json_old = ', '.join(f'b.`{col}` AS `{col}`' for col in fields)
        checks = [
            f"COUNTIF(s.`{key}` IS NULL OR TRIM(s.`{key}`) = '') AS missing_keys",
            f'COUNT(*) - COUNT(DISTINCT s.`{key}`) AS duplicate_keys',
        ]
        for f in target_fields:
            if f.field_type.upper() != 'STRING':
                expr = expressions[f.name]
                checks.append(
                    f"COUNTIF(NULLIF(TRIM(s.`{f.name}`), '') IS NOT NULL "
                    f'AND {expr} IS NULL) AS bad_{f.name}'
                )
        validation = dict(next(iter(client.query(
            f'SELECT {", ".join(checks)} FROM `{stage_id}` s'
        ).result())).items())
        if any(validation.values()):
            raise ValueError(f'{name}: staging validation failed: {validation}')
        old_keys = next(iter(client.query(
            f'SELECT COUNT(*) - COUNT(DISTINCT `{key}`) AS duplicates, '
            f"COUNTIF(`{key}` IS NULL OR TRIM(CAST(`{key}` AS STRING)) = '') AS missing "
            f'FROM `{target_id}`'
        ).result()))
        if old_keys.duplicates or old_keys.missing:
            raise ValueError(f'{name}: existing Bronze contains duplicate/missing keys')
        sql = f'''
        WITH incoming AS (SELECT {typed_select} FROM `{stage_id}` s),
        compared AS (
          SELECT b.`{key}` IS NULL AS is_new,
                 TO_JSON_STRING(STRUCT({json_in})) AS incoming_row,
                 TO_JSON_STRING(STRUCT({json_old})) AS existing_row
          FROM incoming i LEFT JOIN `{target_id}` b ON i.`{key}` = b.`{key}`
        )
        SELECT COUNTIF(is_new) AS new_records,
               COUNTIF(NOT is_new AND incoming_row != existing_row) AS changed_records,
               COUNTIF(NOT is_new AND incoming_row = existing_row) AS unchanged_records
        FROM compared
        '''
        result = dict(next(iter(client.query(sql).result())).items())
        summary[name] = result
        print(f'COMPARE {name}: {result}')
    return summary
