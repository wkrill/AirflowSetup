import logging

from airflow import AirflowException
from airflow.hooks.base import BaseHook

def run_backup(
        conn_id='as4data',
        schema_only=True,
        compression_level: int=0,
        file='/tmp/as4data_back_up.sql'
    ):
    '''Run backup of postgres database and save to file.

    Args:
        conn_id (str, optional): Defaults to 'as4data'.
        schema_only (bool, optional):
            If true only back up structure: schema definitions, functions etc.
            Defaults to True.
        compression_level (int, optional):
            The compression level to use (0-9). Zero means no compression.
            Defaults to 0.
        file (str, optional):
            Path to backup file.
            Defaults to '/tmp/as4data_back_up.sql'.

    Raises:
        AirflowException: If backup fails.
    '''
    import subprocess
    schema_only = '--schema-only' if schema_only else ''

    uri = BaseHook.get_connection(conn_id).get_uri()
    bash_command = (
        f'pg_dump {schema_only} '
        f'--compress={compression_level} '
        f'--dbname={uri} '
        f'--file={file}'
    )
    logging.info('Running command: ' + bash_command)
    output = subprocess.run(bash_command, shell=True, check=True, text=True)
    logging.info(f'Return code: {output.returncode}')
    if output.returncode != 0:
        raise AirflowException
