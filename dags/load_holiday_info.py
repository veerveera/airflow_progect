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

def log_start(proc_name, message, hook):

    start_ts = datetime.now()
    log_id = hook.get_first( f"INSERT INTO {SCHEMA_LOGS}.etl_log (dag_id, start_ts, status, message) VALUES (%s, %s, %s, %s) RETURNING id;", parameters=(proc_name, start_ts, 'Running', message))[0]

    return log_id


def log_end(log_id, status, error_message, hook):

    end_ts = datetime.now()
    if status == 'Success':
        sql = f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s WHERE id = %s"
        hook.run(sql, parameters=(end_ts, 'Success', log_id))
    else:
        sql = f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s, message = %s WHERE id = %s"
        hook.run(sql, parameters=(end_ts, 'Error', str(error_message)[:200], log_id))



def truncate_product_table(**kwargs):
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    calc_date = kwargs.get('ds')

    log_id = log_start('Truncate_product', f'Очистка rd.product за дату {calc_date}', hook)

    try:
        logging.info("Полная очистка таблицы справочника продуктов: rd.product")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE rd.product RESTART IDENTITY CASCADE;")

        log_end(log_id, 'Success', None, hook)
    except Exception as e:
        log_end(log_id, 'Error', e, hook)
        raise e


def load_table_from_csv(table_name, **kwargs):
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    calc_date = kwargs.get('ds')

    proc_name = 'Fill_deal_info' if table_name == 'rd.deal_info' else 'Fill_product_info'

    log_id = log_start(proc_name, f'Загрузка {table_name} из CSV за дату {calc_date}', hook)

    try:
        engine = hook.get_sqlalchemy_engine()
        file_path = FILES_TO_LOAD[table_name]

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не найден по пути: {file_path}")

        df = None
        for enc in ['utf-8', 'cp1251', 'windows-1251', 'latin1']:
            try:
                df = pd.read_csv(file_path, sep=None, engine='python', encoding=enc)
                break
            except Exception:
                continue

        if df is None:
            raise ValueError(f"Не удалось прочитать файл {file_path} ни в одной из кодировок.")

        if '.' in table_name:
            schema, table = table_name.split('.', 1)
        else:
            schema, table = 'public', table_name

        logging.info(f"Импорт данных из файла {file_path} в {table_name} (режим: append)")

        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False,
            chunksize=5000,
            method='multi'
        )

        log_end(log_id, 'Success', None, hook)

    except Exception as e:
        log_end(log_id, 'Error', e, hook)
        raise e


def fill_loan_holiday_info(**kwargs):
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    calc_date = kwargs.get('ds')
    proc_name = 'fill_loan_holiday_info'

    log_id = log_start(proc_name, f'Расчет витрины за дату {calc_date}', hook)

    try:

        hook.run(f"CALL dm.{proc_name}();")

        log_end(log_id, 'Success', None, hook)

    except Exception as e:
        log_end(log_id, 'Error', e, hook)
        raise e



with DAG(
        dag_id='loan_holiday_info',
        start_date=datetime(2026, 5, 1),
        schedule=None,
        catchup=False,
        tags=['datamart', 'clean_architecture']
) as dag:
    start = EmptyOperator(task_id='start')

    truncate_product = PythonOperator(
        task_id='Truncate_product',
        python_callable=truncate_product_table
    )

    fill_deal = PythonOperator(
        task_id='Fill_deal_info',
        python_callable=load_table_from_csv,
        op_kwargs={'table_name': 'rd.deal_info'}
    )

    fill_product = PythonOperator(
        task_id='Fill_product_info',
        python_callable=load_table_from_csv,
        op_kwargs={'table_name': 'rd.product'}
    )

    rebuild_dm = PythonOperator(
        task_id='rebuild_dm_loan_holiday_info',
        python_callable=fill_loan_holiday_info
    )

    end = EmptyOperator(task_id='end')

    start >> [truncate_product, fill_deal]
    truncate_product >> fill_product
    [fill_deal, fill_product] >> rebuild_dm >> end
