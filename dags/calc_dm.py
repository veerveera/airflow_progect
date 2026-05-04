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
    log_id = hook.get_first(f"INSERT INTO {SCHEMA_LOGS}.etl_log (dag_id, start_ts, status, message) VALUES (%s, %s, %s, %s) RETURNING id;", parameters=(proc_name, start_ts, 'Running', f'Расчет за дату {calc_date}'))[0]

    try:
        hook.run(f"CALL ds.{proc_name}(%s)", parameters=(calc_date,))
        hook.run(f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s WHERE id = %s",parameters=(datetime.now(), 'Success', log_id))
    except Exception as e:
        hook.run(f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s, message = %s WHERE id = %s",parameters=(datetime.now(), 'Error', str(e)[:200], log_id))
        raise e


def calculate_turnover(conn_id, **kwargs):
    hook = PostgresHook(postgres_conn_id=conn_id)
    current_date = datetime(2018, 1, 1).date()
    end_date = datetime(2018, 1, 31).date()

    while current_date <= end_date:
        run_log('fill_account_turnover_f', current_date, hook)
        current_date += timedelta(days=1)


def calculate_balance(conn_id, **kwargs):
    hook = PostgresHook(postgres_conn_id=conn_id)
    current_date = datetime(2017, 12, 31).date()
    end_date = datetime(2018, 1, 31).date()

    while current_date <= end_date:
        run_log('fill_account_balance_f', current_date, hook)
        current_date += timedelta(days=1)

with DAG(
        dag_id='calc_dm',
        start_date=datetime(2023, 1, 1),
        schedule=None,
        catchup=False
) as dag:
    start = EmptyOperator(task_id='start')

    turnover_task = PythonOperator(
        task_id='dm_fill_account_turnover_f',
        python_callable=calculate_turnover,
        op_kwargs={'conn_id': CONNECTION_ID}
    )

    balance_task = PythonOperator(
        task_id='dm_fill_account_balance_f',
        python_callable=calculate_balance,
        op_kwargs={'conn_id': CONNECTION_ID}
    )

    end = EmptyOperator(task_id='end')

    start >> turnover_task >> balance_task >> end