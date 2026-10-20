"""Synthetic incremental CSV generator for Cloud Run Jobs.

Read-only BigQuery Bronze baseline; generate approximately 90% unchanged,
7% modified, 3% new rows per table. Synthetic testing only.
"""
import csv
import hashlib
import os
import random
import tempfile
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery, storage

PROJECT_ID = os.getenv('PROJECT_ID', 'vaarahigcpproject')
BRONZE_DATASET = os.getenv('BRONZE_DATASET', 'paraxel_bronze')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'personalproject_1')
BATCH_ID = os.getenv('BATCH_ID') or datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S') + '_' + uuid.uuid4().hex[:8]
GCS_PREFIX = f'clinical_source/{BATCH_ID}'

# (primary key, field to change, alternative values)
TABLES = {
    'clinical_studies': ('study_id', 'study_status', ['Active', 'Completed', 'Pending']),
    'clinical_sites': ('site_id', 'site_status', ['Active', 'Completed', 'Pending']),
    'investigators': ('investigator_id', 'training_status', ['Active', 'Completed', 'Pending']),
    'patients': ('patient_id', 'patient_status', ['Active', 'Completed', 'Pending']),
    'patient_enrollments': ('enrollment_id', 'enrollment_status', ['Active', 'Completed', 'Pending']),
    'patient_visits': ('visit_id', 'visit_status', ['Active', 'Completed', 'Pending']),
    'lab_results': ('lab_result_id', 'abnormal_flag', ['Y', 'N']),
    'vital_signs': ('vital_id', 'heart_rate', ['68', '72', '78', '84', '90']),
    'adverse_events': ('ae_id', 'severity', ['Mild', 'Moderate', 'Severe']),
    'medications': ('medication_id', 'frequency', ['Daily', 'Twice daily']),
    'medical_history': ('history_id', 'condition_status', ['Active', 'Completed', 'Pending']),
    'study_treatments': ('treatment_id', 'treatment_status', ['Active', 'Completed', 'Pending']),
}
META = {'batch_id', 'source_file', 'ingestion_timestamp'}


def alternate_value(old, options, rng):
    choices = [v for v in options if v != (old or '')]
    if not choices:
        raise ValueError('No alternate value is available')
    return rng.choice(choices)


def main():
    bq = bigquery.Client(project=PROJECT_ID)
    gcs = storage.Client(project=PROJECT_ID)
    bucket = gcs.bucket(BUCKET_NAME)
    seed = int.from_bytes(hashlib.sha256(BATCH_ID.encode()).digest()[:8], 'big')
    rng = random.Random(seed)
    grand_total = 0

    with tempfile.TemporaryDirectory(prefix='clinical_incremental_') as directory:
        for name, (key, change_field, options) in TABLES.items():
            table_id = f'{PROJECT_ID}.{BRONZE_DATASET}.{name}'
            table = bq.get_table(table_id)
            columns = [f.name for f in table.schema if f.name not in META]
            if key not in columns or change_field not in columns:
                raise ValueError(f'{name}: missing primary key or change field in Bronze')
            if any(f.field_type != 'STRING' for f in table.schema if f.name in columns):
                raise ValueError(f'{name}: expected all source fields to be STRING in Bronze')

            select_cols = ', '.join(f'`{c}`' for c in columns)
            query = f'SELECT {select_cols} FROM `{table_id}` ORDER BY `{key}`'
            records = [{c: '' if row[c] is None else str(row[c]) for c in columns}
                       for row in bq.query(query).result()]
            if not records:
                raise ValueError(f'{name}: Bronze baseline is empty')
            keys = [r[key] for r in records]
            if any(not k.strip() for k in keys) or len(keys) != len(set(keys)):
                raise ValueError(f'{name}: missing or duplicate primary keys in Bronze')

            n = len(records)
            new_count = round(n * 0.03)
            changed_count = round(n * 0.07)
            existing_count = n - new_count
            if existing_count < changed_count:
                raise ValueError(f'{name}: not enough baseline rows')

            selected = rng.sample(range(n), existing_count)
            changed = set(rng.sample(selected, changed_count))
            output = []
            for index in selected:
                record = records[index].copy()
                if index in changed:
                    record[change_field] = alternate_value(record[change_field], options, rng)
                output.append(record)

            used_keys = set(keys)
            for j in range(new_count):
                record = records[rng.randrange(n)].copy()
                suffix = hashlib.sha256(f'{BATCH_ID}:{name}:{j}'.encode()).hexdigest()[:16].upper()
                new_key = f'NEW_{suffix}'
                if new_key in used_keys:
                    raise ValueError(f'{name}: generated primary key collision')
                used_keys.add(new_key)
                record[key] = new_key
                output.append(record)

            rng.shuffle(output)
            path = os.path.join(directory, f'{name}.csv')
            with open(path, 'w', newline='', encoding='utf-8') as fp:
                writer = csv.DictWriter(fp, fieldnames=columns)
                writer.writeheader()
                writer.writerows(output)

            blob_path = f'{GCS_PREFIX}/{name}.csv'
            bucket.blob(blob_path).upload_from_filename(path, content_type='text/csv')
            grand_total += len(output)
            print(f'UPLOADED {name}: total={len(output)}, unchanged={existing_count-changed_count}, '
                  f'changed={changed_count}, new={new_count} -> gs://{BUCKET_NAME}/{blob_path}', flush=True)

    print(f'COMPLETE batch={BATCH_ID} tables={len(TABLES)} total_rows={grand_total}', flush=True)


if __name__ == '__main__':
    main()
