import os
import logging
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONNECTION_ID = 'postgres_db'
SCHEMA_LOGS = 'logs'

CSV_FOLDER = '/opt/airflow/data/'
FILES_TO_LOAD = {
    'rd.deal_info': os.path.join(CSV_FOLDER, 'deal_info.csv'),
    'rd.product': os.path.join(CSV_FOLDER, 'product_info.csv')
}


def run_log(proc_name, calc_date, hook):
    start_ts = datetime.now()
    log_id = hook.get_first(
        f"INSERT INTO {SCHEMA_LOGS}.etl_log (dag_id, start_ts, status, message) "
        f"VALUES (%s, %s, %s, %s) RETURNING id;",
        parameters=(proc_name, start_ts, 'Running', f'Расчет за дату {calc_date}')
    )[0]

    try:
        if proc_name == 'fill_loan_holiday_info':
            hook.run(f"CALL dm.{proc_name}();")
        else:
            hook.run(f"CALL dm.{proc_name}(%s);", parameters=(calc_date,))

        hook.run(
            f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s WHERE id = %s",
            parameters=(datetime.now(), 'Success', log_id)
        )

    except Exception as e:
        hook.run(
            f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s, message = %s WHERE id = %s",
            parameters=(datetime.now(), 'Error', str(e)[:200], log_id)
        )
        raise e


def load_sources_from_csv(**kwargs):
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    engine = hook.get_sqlalchemy_engine()

    for table_name, file_path in FILES_TO_LOAD.items():
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не найден по пути: {file_path}")

        df = None
        for enc in ['utf-8', 'cp1251', 'windows-1251', 'latin1']:
            try:
                df = pd.read_csv(file_path, sep=None, engine='python', encoding=enc)
                break
            except Exception as e:
                continue

        if df is None:
            raise ValueError(f"Не удалось прочитать файл {file_path} ни в одной из кодировок.")

        if '.' in table_name:
            schema, table = table_name.split('.', 1)
        else:
            schema, table = 'public', table_name

        logging.info(f"Очистка таблицы детального слоя: {table_name}")
        hook.run(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")

        logging.info(f"Импорт свежих данных из файла {file_path}")

        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False,
            chunksize=10000,
            method='multi'
        )


def calculate_target_datamart(**kwargs):
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    calc_date = kwargs.get('ds')
    run_log(proc_name='fill_loan_holiday_info', calc_date=calc_date, hook=hook)


with DAG(
        dag_id='loan_holiday_info',
        start_date=datetime(2026, 5, 1),
        schedule=None,
        catchup=False,
        tags=['datamart', 'clean_architecture']
) as dag:
    start = EmptyOperator(task_id='start')

    load_csv = PythonOperator(
        task_id='load_csv_sources',
        python_callable=load_sources_from_csv
    )

    rebuild_dm = PythonOperator(
        task_id='rebuild_dm_loan_holiday_info',
        python_callable=calculate_target_datamart
    )

    end = EmptyOperator(task_id='end')

    start >> load_csv >> rebuild_dm >> end