import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONNECTION_ID = 'postgres_db'
SCHEMA_LOGS = 'logs'


def run_log(proc_name, hook):
    start_ts = datetime.now()

    log_id = hook.get_first(f"INSERT INTO {SCHEMA_LOGS}.etl_log (dag_id, start_ts, status, message) VALUES (%s, %s, %s, %s) RETURNING id;",parameters=(proc_name, start_ts, 'Running', f'Запуск процедуры {proc_name} без параметров'))[0]

    try:
        hook.run(f"CALL dm.{proc_name}();")

        hook.run(f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s, message = %s WHERE id = %s", parameters=(datetime.now(), 'Success', 'Данные успешно скорректированы и перезагружены в витрину', log_id))
    except Exception as e:

        hook.run(f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s, message = %s WHERE id = %s", parameters=(datetime.now(), 'Error', f"Ошибка: {str(e)[:150]}", log_id))
        raise e


def fill_account_balance_turnover_func(**kwargs):

    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    run_log(proc_name='fill_account_balance_turnover',hook=hook)


with DAG(
        dag_id='account_balance',
        schedule_interval=None,
        start_date=datetime(2026, 5, 1),
        catchup=False,
        max_active_runs=1,
        tags=['finance', 'dm', 'rd']
) as dag:

    start = EmptyOperator(task_id='start')


    fill_turnover_vjtrina = PythonOperator(
        task_id='fill_account_balance_turnover',
        python_callable=fill_account_balance_turnover_func
    )

    end = EmptyOperator(task_id='end')


    start >> fill_turnover_vjtrina >> end