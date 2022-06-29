from datetime import datetime, timedelta
from airflow import AirflowException
from dateutil import parser
import logging

from airflow.decorators import dag, task
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'whk',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'email': ['whk@brejnholt.dk'],
    'email_on_failure': True,
    'email_on_retry': False,
}

def _is_work_day_hours(ts):
    # Decides if execution time (ts) is during work hours 05:00-17:00 CET
    from dateutil import tz
    local_tz = tz.gettz('Europe/Copenhagen')

    local_data_interval_start = parser.parse(ts).astimezone(local_tz)

    # Execution happens 1 hour after data_interval_start
    return 4 <= local_data_interval_start.hour <= 16


def log_cron(hook, tablename, schedule, method, start, end, status):
    hook.run(
        "INSERT INTO as4mirror.cronlog "
        "(mastertable, schedule, method, starttime, endtime, status) "
        f"VALUES ('{tablename}', 'Airflow_{schedule}', '{method}', '{start}', '{end}', '{status}')"
    )

@dag(default_args=default_args,
     schedule_interval='40 * * * *',
     start_date=datetime(2022, 6, 1),
     catchup=False)
def refresh_as4mirror_hourly():

    is_work_day_hours = ShortCircuitOperator(
        task_id="is_work_day_hours",
        python_callable=_is_work_day_hours
    )

    @task
    def get_joblist():
        '''Get list of tables that should be refreshed hourly and their refresh methods'''
        hook = PostgresHook('postgres_default')
        cur = hook.get_cursor()
        REFRESH_METHOD = 'refresh_hourly'
        sql = (
            f"SELECT tablename, {REFRESH_METHOD} from mirror_plan "
            f"WHERE {REFRESH_METHOD} IS NOT NULL"
        )
        cur.execute(sql)
        res = cur.fetchall()
        joblist = list(res)
        return joblist
    
    @task(max_active_tis_per_dag=8)
    def refresh(arg):
        from dateutil import tz
        tbl, method = arg
        local_tz = tz.gettz('Europe/Copenhagen')

        start = datetime.now().astimezone(local_tz)

        try:
            hook = PostgresHook('postgres_default')
            msg = f"Run refresh for: {tbl} {method}"
            logging.info(msg)
            hook.run(
                f"CALL refresh_with_method('{tbl}','{method}')",
                autocommit=True
            )
            for output in hook.conn.notices:
                logging.info(output.rstrip('\n'))
            status = "ok"
        except Exception as e:
            status = str(e)

        end = datetime.now().astimezone(local_tz)
        log_cron(hook, tbl, "Daily", method, start, end, status)
        if status != 'ok':
            raise AirflowException(f"{tbl} refresh failed! Error message: {status}")
        else:
            logging.info(f'Successfully refreshed {tbl}')

    joblist = get_joblist()
    refresh_all = refresh.expand(arg=joblist)   # Dynamic task mapping
    is_work_day_hours >> [refresh_all, joblist]

dag = refresh_as4mirror_hourly()
