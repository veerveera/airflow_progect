import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONNECTION_ID = 'postgres_db'
SCHEMA_LOGS = 'logs'


def run_log(proc_name, calc_date, hook):

    start_ts = datetime.now()
    log_id = hook.get_first( f"INSERT INTO {SCHEMA_LOGS}.etl_log (dag_id, start_ts, status, message) VALUES (%s, %s, %s, %s) RETURNING id;",parameters=(proc_name, start_ts, 'Running', f'Расчет за дату {calc_date}'))[0]

    try:
        delete_sql = """
        DO $$
        BEGIN
            DELETE FROM dm.client
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM dm.client
                GROUP BY client_rk, effective_from_date
            );
        END $$;
        """

        hook.run(delete_sql)

        hook.run(f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s WHERE id = %s", parameters=(datetime.now(), 'Success', log_id))

    except Exception as e:
        hook.run(f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s, message = %s WHERE id = %s", parameters=(datetime.now(), 'Error', str(e)[:200], log_id))
        raise e


def execute_deduplication(**kwargs):
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    calc_date = kwargs.get('ds')

    run_log(proc_name='delete_client_duplicates', calc_date=calc_date, hook=hook)


with DAG(
        dag_id='dm_client_deduplication',
        start_date=datetime(2026, 5, 1),
        schedule=None,
        catchup=False,
        tags=['cleaning', 'dm']
) as dag:
    start = EmptyOperator(task_id='start')

    remove_duplicates_task = PythonOperator(
        task_id='delete_client_duplicates',
        python_callable=execute_deduplication
    )

    end = EmptyOperator(task_id='end')

    start >> remove_duplicates_task >> end