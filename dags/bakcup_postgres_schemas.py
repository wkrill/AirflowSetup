from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.python import PythonOperator

from operators.s3 import UploadFilesToS3Operator
from util.backup import run_backup


default_args = {
    'owner': 'whk',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'email': ['whk@brejnholt.dk'],
    'email_on_failure': True,
    'email_on_retry': False,
}

@dag(default_args=default_args,
     schedule_interval='37 1 * * SUN',
     start_date=datetime(2022, 6, 1),
     catchup=False,
     tags=['maintenance', 'backup','as4data'])
def postgres_backup_structure():
    '''
    ### Back up as4data database structure on S3.
    Back-up file does not include any data, only table definitions, users,
    functions views etc.

    NOTICE: Requires postgres-client installed.
    If in doubt check for `apt-get install -y postgres-client` in `Dockerfile`. 
    '''
    t1 = PythonOperator(
        task_id='back_up_schemas',
        python_callable=run_backup,
        op_kwargs={
            'conn_id': 'as4data',
            'schema_only': True,
            'compression_level': 9,
            'file': '/tmp/as4data_backup_schemas.sql'
        }
    )

    t2 = UploadFilesToS3Operator(
        task_id='upload',
        aws_conn_id='aws_default',
        bucket_name='dk.brejnholt.postgresql-backup',
        files=['/tmp/as4data_backup_schemas.sql'],
        keys=['as4data_backup_schemas_{{ data_interval_end | ds_nodash }}.sql'],
        replace=True
    )

    t1 >> t2

dag = postgres_backup_structure()
