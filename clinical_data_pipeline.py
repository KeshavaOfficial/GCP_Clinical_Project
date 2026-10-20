"""Clinical API pipeline and guarded hourly 12-table incremental MERGE and Silver sync.

Modes:
  normal: generator -> validation -> staging -> compare -> dry-run -> backup ->
          live Bronze MERGE -> Silver MERGE.
  test_existing_batch: read-only processing of supplied batch_id.
  apply_existing_batch: live processing only with confirm_live_merge=true.
Keep DAG paused until code and deployment have been tested.
"""
from datetime import timedelta
import re
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

from trigger_flask_api import trigger_flask_api
from read_pipeline_config import read_pipeline_config
from check_gcs_file import check_gcs_file
from load_gcs_to_bronze import load_gcs_to_bronze
from validate_clinical_files import validate_clinical_files
from load_clinical_staging import load_clinical_staging
from compare_clinical_incremental import compare_clinical_incremental
from merge_clinical_incremental import merge_clinical_incremental
from archive_bronze_before_merge import archive_bronze_before_merge, batch_backup_suffix
from bronze_to_silver import process_bronze_to_silver
from read_extra_config import read_extra_config
from audit_start import audit_start
from transform_bronze_to_silver import transform_bronze_to_silver
from audit_success import audit_success
from audit_failure import audit_failure

PROJECT_ID = 'vaarahigcpproject'
REGION = 'us-central1'
DEFAULT_TEST_BATCH = '20261019_204010_7fc73533'
BATCH_TEMPLATE = "{{ dag_run.conf.get('batch_id', 'clinical_' ~ ts_nodash ~ '_' ~ dag_run.run_type) }}"


def get_current_batch_id(context):
    conf = context['dag_run'].conf or {}
    mode = conf.get('mode', 'normal')
    if mode == 'normal':
        if conf.get('batch_id'):
            raise ValueError('Normal mode cannot override batch_id')
        batch_id = f"clinical_{context['ts_nodash']}_{context['dag_run'].run_type}"
    elif mode in ('test_existing_batch', 'apply_existing_batch'):
        batch_id = conf.get('batch_id', DEFAULT_TEST_BATCH)
    else:
        raise ValueError(f'Unknown mode: {mode}')
    if not isinstance(batch_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]+', batch_id):
        raise ValueError('Invalid batch_id')
    return batch_id


def route_execution(**context):
    conf = context['dag_run'].conf or {}
    mode = conf.get('mode', 'normal')
    get_current_batch_id(context)
    if mode in ('test_existing_batch', 'apply_existing_batch'):
        if mode == 'apply_existing_batch' and conf.get('confirm_live_merge') is not True:
            raise ValueError('Live MERGE requires confirm_live_merge=true')
        return 'existing_batch_ready'
    return ['trigger_flask_api', 'trigger_clinical_generator']


def validate_batch_id(**context):
    batch_id = get_current_batch_id(context)
    print(f'Processing existing batch: {batch_id}')


def require_full_compare(**context):
    result = context['ti'].xcom_pull(task_ids='compare_clinical_incremental')
    if not isinstance(result, dict) or len(result) != 12:
        raise ValueError(f'Expected comparison results for 12 tables; received {result!r}')
    totals = {k: sum(int(v.get(k, 0) or 0) for v in result.values())
              for k in ('new_records', 'changed_records', 'unchanged_records')}
    print(f'Incremental comparison totals: {totals}')
    return totals


def run_merge_dry_run(**context):
    return merge_clinical_incremental(batch_id=get_current_batch_id(context), dry_run=True)


def verify_dry_run_totals(**context):
    comparison = context['ti'].xcom_pull(task_ids='summarize_incremental_comparison')
    dry_run = context['ti'].xcom_pull(task_ids='merge_clinical_dry_run')
    if not isinstance(comparison, dict) or not isinstance(dry_run, dict):
        raise ValueError('Missing comparison or MERGE dry-run totals')
    for key in ('new_records', 'changed_records', 'unchanged_records'):
        if int(comparison[key]) != int(dry_run[key]):
            raise ValueError(f'MERGE dry-run differs from comparison for {key}: '
                             f'{comparison[key]} vs {dry_run[key]}')
    print(f'MERGE dry-run verified against comparison: {dry_run}')
    return dry_run


def choose_merge_execution(**context):
    conf = context['dag_run'].conf or {}
    mode = conf.get('mode', 'normal')
    if mode == 'normal' or (mode == 'apply_existing_batch' and conf.get('confirm_live_merge') is True):
        return 'archive_bronze_before_merge'
    return 'skip_live_merge'


def run_merge_live(**context):
    conf = context['dag_run'].conf or {}
    mode = conf.get('mode', 'normal')
    if mode == 'apply_existing_batch' and conf.get('confirm_live_merge') is not True:
        raise ValueError('Manual live MERGE requires confirmation')
    if mode not in ('normal', 'apply_existing_batch'):
        raise ValueError(f'Live MERGE forbidden in mode: {mode}')
    batch_id = get_current_batch_id(context)
    return merge_clinical_incremental(batch_id=batch_id, dry_run=False,
                                      backup_suffix=batch_backup_suffix(batch_id))


def run_archive(**context):
    return archive_bronze_before_merge(batch_id=get_current_batch_id(context))


def run_clinical_silver(**context):
    conf = context['dag_run'].conf or {}
    mode = conf.get('mode', 'normal')
    if mode not in ('normal', 'apply_existing_batch'):
        raise ValueError(f'Silver MERGE forbidden in mode: {mode}')
    if mode == 'apply_existing_batch' and conf.get('confirm_live_merge') is not True:
        raise ValueError('Manual Silver MERGE requires confirmation')
    get_current_batch_id(context)
    return process_bronze_to_silver()


default_args = {
    'owner': 'clinical_data_team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': audit_failure,
}

with DAG(
    dag_id='clinical_data_pipeline',
    default_args=default_args,
    description='Clinical API Bronze/Silver and guarded hourly 12-table incremental MERGE',
    start_date=pendulum.datetime(2026, 10, 17, tz='UTC'),
    schedule='0 * * * *',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['clinical', 'gcp', 'bigquery', 'cloud-run'],
) as dag:
    start = EmptyOperator(task_id='start')
    route = BranchPythonOperator(task_id='route_execution', python_callable=route_execution)
    existing_batch_ready = PythonOperator(task_id='existing_batch_ready', python_callable=validate_batch_id)

    generate_api = PythonOperator(task_id='trigger_flask_api', python_callable=trigger_flask_api)
    read_config = PythonOperator(task_id='read_pipeline_config', python_callable=read_pipeline_config)
    read_extra = PythonOperator(task_id='read_extra_config', python_callable=read_extra_config)
    check_file = PythonOperator(task_id='check_gcs_file', python_callable=check_gcs_file)
    audit_task = PythonOperator(task_id='audit_start', python_callable=audit_start)
    load_bronze = PythonOperator(task_id='load_gcs_to_bronze', python_callable=load_gcs_to_bronze)
    transform_silver = PythonOperator(task_id='transform_bronze_to_silver', python_callable=transform_bronze_to_silver)
    audit_complete = PythonOperator(task_id='audit_success', python_callable=audit_success)

    generate_clinical = CloudRunExecuteJobOperator(
        task_id='trigger_clinical_generator',
        project_id=PROJECT_ID,
        region=REGION,
        job_name='clinical-data-generator',
        overrides={'container_overrides': [{'env': [{'name': 'BATCH_ID', 'value': BATCH_TEMPLATE}]}]},
        deferrable=False,
        gcp_conn_id='google_cloud_default',
    )
    validate_clinical = PythonOperator(
        task_id='validate_clinical_files',
        python_callable=validate_clinical_files,
        op_kwargs={'batch_id': BATCH_TEMPLATE},
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    stage_clinical = PythonOperator(
        task_id='load_clinical_staging',
        python_callable=load_clinical_staging,
        op_kwargs={'batch_id': BATCH_TEMPLATE},
    )
    compare_clinical = PythonOperator(task_id='compare_clinical_incremental', python_callable=compare_clinical_incremental)
    compare_totals = PythonOperator(task_id='summarize_incremental_comparison', python_callable=require_full_compare)
    merge_dry_run = PythonOperator(task_id='merge_clinical_dry_run', python_callable=run_merge_dry_run)
    verify_merge_dry_run = PythonOperator(task_id='verify_merge_dry_run', python_callable=verify_dry_run_totals)
    choose_merge = BranchPythonOperator(task_id='choose_merge_execution', python_callable=choose_merge_execution)
    backup_bronze = PythonOperator(task_id='archive_bronze_before_merge', python_callable=run_archive)
    merge_live = PythonOperator(task_id='merge_clinical_live', python_callable=run_merge_live)
    clinical_silver = PythonOperator(task_id='process_clinical_silver', python_callable=run_clinical_silver)
    skip_live = EmptyOperator(task_id='skip_live_merge')
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    start >> route
    route >> generate_api >> read_config >> read_extra >> check_file >> audit_task >> load_bronze >> transform_silver >> audit_complete >> end
    route >> generate_clinical >> validate_clinical
    route >> existing_batch_ready >> validate_clinical
    validate_clinical >> stage_clinical >> compare_clinical >> compare_totals >> merge_dry_run >> verify_merge_dry_run >> choose_merge
    choose_merge >> backup_bronze >> merge_live >> clinical_silver >> end
    choose_merge >> skip_live >> end
