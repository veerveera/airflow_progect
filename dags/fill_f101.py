import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONNECTION_ID = 'postgres_db'
SCHEMA_LOGS = 'logs'

def run_log(proc_name, calc_date, hook):
    start_ts = datetime.now()
    log_id = hook.get_first(
        f"INSERT INTO {SCHEMA_LOGS}.etl_log (dag_id, start_ts, status, message) VALUES (%s, %s, %s, %s) RETURNING id;",
        parameters=(proc_name, start_ts, 'Running', f'Расчет за дату {calc_date}'))[0]

    try:
        hook.run(f"CALL dm.{proc_name}(%s::DATE)", parameters=(calc_date,))
        hook.run(f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s WHERE id = %s", parameters=(datetime.now(), 'Success', log_id))
    except Exception as e:
        hook.run(f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s, message = %s WHERE id = %s", parameters=(datetime.now(), 'Error', str(e)[:200], log_id))
        raise e

def fill_f101_round_f_func(ds, **kwargs):
    hook = PostgresHook(postgres_conn_id = CONNECTION_ID)
    fixed_date = '2018-02-01'
    run_log(
        proc_name = 'fill_f101_round_f',
        calc_date = fixed_date,
        hook = hook
    )

with DAG(
    dag_id='f101',
    schedule=None,
    start_date=datetime(2018, 2, 1),
    catchup=False,
    max_active_runs=1
) as dag:
    start = EmptyOperator(task_id='start')

    fill_f101 = PythonOperator(
        task_id = 'fill_f101_round_f',
        python_callable = fill_f101_round_f_func
    )

    end = EmptyOperator(task_id='end')

    start >> fill_f101 >> end