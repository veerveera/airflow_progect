import csv
import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONNECTION_ID = 'postgres_db'
SCHEMA_LOGS = 'logs'
CSV_PATH = '/opt/airflow/data/dm_f101_export.csv'

CSV_DELIMITER = ';'


def export_f101_to_csv(**kwargs):
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    calc_date = kwargs.get('ds')

    start_ts = datetime.now()
    log_id = hook.get_first(
        f"INSERT INTO {SCHEMA_LOGS}.etl_log (dag_id, start_ts, status, message) VALUES (%s, %s, %s, %s) RETURNING id;",
        parameters=('export_f101_csv', start_ts, 'Running', f'Выгрузка в CSV за {calc_date}')
    )[0]

    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dm.dm_f101_round_f")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

        with open(CSV_PATH, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f, delimiter=CSV_DELIMITER)
            writer.writerow(columns)
            writer.writerows(rows)

        hook.run(f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s, message = %s WHERE id = %s",
                 parameters=(datetime.now(), 'Success', f'Выгружено строк: {len(rows)}', log_id))
    except Exception as e:
        hook.run(f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s, message = %s WHERE id = %s",
                 parameters=(datetime.now(), 'Error', str(e)[:200], log_id))
        raise e


def import_f101_v2_from_csv(**kwargs):
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    calc_date = kwargs.get('ds')

    start_ts = datetime.now()
    log_id = hook.get_first(
        f"INSERT INTO {SCHEMA_LOGS}.etl_log (dag_id, start_ts, status, message) VALUES (%s, %s, %s, %s) RETURNING id;",
        parameters=('import_f101_v2', start_ts, 'Running', f'Импорт из CSV за {calc_date}')
    )[0]

    try:
        hook.run("CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2 (LIKE dm.dm_f101_round_f INCLUDING ALL)")
        hook.run("TRUNCATE TABLE dm.dm_f101_round_f_v2")

        conn = hook.get_conn()
        cursor = conn.cursor()

        with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
            copy_sql = f"COPY dm.dm_f101_round_f_v2 FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER '{CSV_DELIMITER}')"
            cursor.copy_expert(copy_sql, f)
        conn.commit()

        hook.run(f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s, message = %s WHERE id = %s",
                 parameters=(datetime.now(), 'Success', 'Данные загружены в v2', log_id))
    except Exception as e:
        hook.run(f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s, message = %s WHERE id = %s",
                 parameters=(datetime.now(), 'Error', str(e)[:200], log_id))
        raise e


with DAG(
        dag_id='f101_csv',
        start_date=datetime(2026, 5, 4),
        schedule=None,
        catchup=False
) as dag:
    start = EmptyOperator(task_id='start')

    export_task = PythonOperator(
        task_id='export_f101_csv',
        python_callable=export_f101_to_csv
    )

    import_task = PythonOperator(
        task_id='import_f101_v2',
        python_callable=import_f101_v2_from_csv
    )

    end = EmptyOperator(task_id='end')

    start >> export_task >> import_task >> end
