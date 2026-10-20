"""Load 12 CSV files into replaceable STRING-only staging tables.
Does not modify existing Bronze business tables.
"""
import csv
import re
from google.cloud import bigquery, storage

PROJECT_ID = 'vaarahigcpproject'
DATASET = 'paraxel_bronze'
BUCKET = 'personalproject_1'
TABLES = (
    'adverse_events', 'clinical_sites', 'clinical_studies', 'investigators',
    'lab_results', 'medical_history', 'medications', 'patient_enrollments',
    'patient_visits', 'patients', 'study_treatments', 'vital_signs',
)


def load_clinical_staging(batch_id: str, **_context):
    if not re.fullmatch(r'[A-Za-z0-9_-]+', batch_id):
        raise ValueError(f'Unsafe batch_id: {batch_id!r}')
    client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)
    results = {}
    for name in TABLES:
        object_name = f'clinical_source/{batch_id}/{name}.csv'
        uri = f'gs://{BUCKET}/{object_name}'
        table_id = f'{PROJECT_ID}.{DATASET}.{name}_staging'
        blob = storage_client.bucket(BUCKET).blob(object_name)
        if not blob.exists(client=storage_client):
            raise FileNotFoundError(uri)
        header_bytes = blob.download_as_bytes(start=0, end=16383)
        if not header_bytes:
            raise ValueError(f'Empty CSV: {uri}')
        header_line = header_bytes.splitlines()[0].decode('utf-8-sig')
        columns = next(csv.reader([header_line]))
        if not columns or any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', c) for c in columns):
            raise ValueError(f'Invalid CSV header in {uri}: {columns}')
        if len(columns) != len(set(columns)):
            raise ValueError(f'Duplicate CSV columns in {uri}')
        # WRITE_TRUNCATE atomically replaces staging on successful load;
        # do not delete the previous table before a load job succeeds.
        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            schema=[bigquery.SchemaField(c, 'STRING') for c in columns],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            allow_quoted_newlines=True,
        )
        client.load_table_from_uri(uri, table_id, job_config=config).result()
        table = client.get_table(table_id)
        if table.num_rows == 0:
            raise ValueError(f'{name}: staging loaded zero rows')
        results[name] = table.num_rows
        print(f'STAGED {name}: {table.num_rows} rows -> {table_id}')
    return results
