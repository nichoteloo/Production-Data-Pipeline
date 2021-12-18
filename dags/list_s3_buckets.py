## list s3 resources accross different az, copy from s3 to redshift

import datetime
import logging
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from plugins.helpers import create_tables

def list_keys():
    hook = S3Hook(aws_conn_id='aws_credentials') ## conn with s3
    bucket = Variable.get('s3_bucket')
    log_prefix = Variable.get('s3_logdata_prefix') ## log_data
    logging.info(f"Listing Keys from {bucket}")
    keys = hook.list_keys(bucket, log_prefix)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}") ## take the s3://<bucket>/log_data/2021/10/

def copy_events_from_s3_to_redshift(*args, **kwargs):
    table = kwargs['params']['table']
    hook = S3Hook(aws_conn_id='aws_credentials')
    redshift_hook = PostgresHook('redshift') ## conn with redshift

    log_data = Variable.get('LOG_DATA') ## copy based on log data
    arn_iam_role = Variable.get('iam_role')
    region = Variable.get('region')
    log_jsonpath = Variable.get('LOG_JSONPATH')
    logging.info(f"Copying from s3 {log_data} to redshift table {table}")

    ## format variables
    sql_stmt = create_tables.COPY_SQL.format(
        table,
        log_data,
        arn_iam_role,
        region,
        log_jsonpath
    )
    logging.info(f"COPY SQL statement is: {sql_stmt}")
    redshift_hook.run(sql_stmt)

dag = DAG(
    'list_s3_bucket',
    start_date = datetime.datetime.utcnow() - timedelta(hours=5)
)

list_task = PythonOperator(
    task_id = "list_keys",
    python_callable = list_keys,
    dag = dag
)

copy_events_from_s3_to_redshift = PythonOperator(
    task_id='copy_events_from_s3_to_redshift',
    python_callable=copy_events_from_s3_to_redshift,
    dag=dag,
    provide_context=True,
    params={
        'table':'staging_events',
    }
)

list_task >> copy_events_from_s3_to_redshift_task