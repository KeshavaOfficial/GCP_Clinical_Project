"""Create immutable pre-MERGE BigQuery table copies for a clinical batch."""
import re
from google.cloud import bigquery
from google.api_core.exceptions import Conflict, NotFound
from merge_clinical_incremental import PROJECT, DATASET, ARCHIVE, KEYS


def batch_backup_suffix(batch_id):
    if not isinstance(batch_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]+', batch_id):
        raise ValueError('Unsafe batch ID')
    return '_premerge_' + batch_id.replace('-', '_')


def archive_bronze_before_merge(batch_id, **_context):
    """Never overwrite a backup. Validate every copy before permitting MERGE."""
    client = bigquery.Client(project=PROJECT)
    suffix = batch_backup_suffix(batch_id)
    result = {}
    for name, key in KEYS.items():
        source = f'{PROJECT}.{DATASET}.{name}'
        destination = f'{PROJECT}.{ARCHIVE}.{name}{suffix}'
        try:
            client.get_table(destination)
            print(f'BACKUP EXISTS {destination}; keeping original', flush=True)
        except NotFound:
            job_config = bigquery.CopyJobConfig(write_disposition='WRITE_EMPTY')
            try:
                client.copy_table(source, destination, job_config=job_config).result()
            except Conflict:
                # Another attempt created the same backup; never overwrite.
                client.get_table(destination)
        # Key-set checks tolerate legitimate new Bronze rows after a partial retry.
        # All backup keys must still exist in Bronze.
        sql = f"""SELECT
            (SELECT COUNT(*) FROM `{destination}`) AS backup_rows,
            (SELECT COUNT(*) FROM `{source}`) AS bronze_rows,
            (SELECT COUNT(*) FROM `{destination}` B
             WHERE NOT EXISTS (SELECT 1 FROM `{source}` T
                               WHERE T.`{key}` = B.`{key}`)) AS missing_keys"""
        r = next(iter(client.query(sql).result()))
        if r.backup_rows <= 0 or r.bronze_rows < r.backup_rows or r.missing_keys:
            raise RuntimeError(f'Invalid backup {name}: {dict(r.items())}')
        print(f'BACKUP VERIFIED {name}: {r.backup_rows} rows', flush=True)
        result[name] = int(r.backup_rows)
    if len(result) != len(KEYS):
        raise RuntimeError('Incomplete backups')
    return {'backup_suffix': suffix, 'table_counts': result}
