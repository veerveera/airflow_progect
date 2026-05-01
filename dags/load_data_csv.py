import pandas as pd
import time
import logging
from sqlalchemy import text
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONNECTION_ID = 'postgres_db'
SCHEMA_DS = 'ds'
SCHEMA_LOGS = 'logs'

TABLE_KEYS = {
    'ft_balance_f': ['on_date', 'account_rk'],
    'md_account_d': ['data_actual_date', 'account_rk'],
    'md_currency_d': ['currency_rk', 'data_actual_date'],
    'md_exchange_rate_d': ['data_actual_date', 'currency_rk'],
    'md_ledger_account_s': ['ledger_account', 'start_date']
}

def insert_data(file_path, table_name, conn_id, **kwargs):
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    start_ts = datetime.now()
    task_instance = kwargs.get('task_instance')
    process_name = task_instance.task_id if task_instance else f"load_{table_name}"

    log_start_query = f"""
        INSERT INTO {SCHEMA_LOGS}.etl_log (dag_id, start_ts, status, message)
        VALUES (%s, %s, %s, %s) RETURNING id;
    """
    log_id = hook.get_first(log_start_query, parameters=(process_name, start_ts, 'Running', f'Начата загрузка файла {file_path}'))[0]

    try:
        time.sleep(5)

        df = None
        for enc in ['utf-8', 'cp1251', 'latin1']:
            try:
                df = pd.read_csv(file_path, sep=';', encoding=enc)
                break
            except:
                continue

        if df is None:
            raise ValueError(f"Не удалось прочитать файл {file_path}")

        df.columns = [c.lower().strip() for c in df.columns]

        for col in df.columns:
            if 'date' in col or 'dt' in col:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce', format='mixed')
            else:
                if df[col].dtype == 'float64':
                    if (df[col].dropna() % 1 == 0).all():
                        df[col] = df[col].fillna(0).astype(int).astype(str)
                        if 'code' in col:
                            df[col] = df[col].str.zfill(3)

                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.strip()
                    df.loc[df[col] == 'nan', col] = None

        df = df.drop_duplicates()

        ##truncate_query = f"TRUNCATE TABLE {SCHEMA_DS}.{table_name} RESTART IDENTITY CASCADE;"
        ##hook.run(truncate_query)
        ##df.to_sql(table_name, engine, schema=SCHEMA_DS, if_exists='append', index=False)

        temp_table = f"temp_{table_name}"

        with engine.begin() as conn:
            if table_name.lower() == 'ft_posting_f':
                conn.execute(text(f"TRUNCATE TABLE {SCHEMA_DS}.{table_name} RESTART IDENTITY CASCADE;"))
                df.to_sql(table_name, conn, schema=SCHEMA_DS, if_exists='append', index=False)
                msg = f"Загружено с нуля: {len(df)} строк"

            else:
                tmp_name = f"tmp_upload_{table_name}"

                df.to_sql(tmp_name, conn, if_exists='replace', index=False)

                keys = TABLE_KEYS[table_name.lower()]
                cols = [f'"{c}"' for c in df.columns]
                update_part = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c not in keys])

                upsert_sql = f"""
                    INSERT INTO {SCHEMA_DS}.{table_name} ({", ".join(cols)})
                    SELECT {", ".join(cols)} FROM {tmp_name}
                    ON CONFLICT ({", ".join([f'"{k}"' for k in keys])}) 
                    DO UPDATE SET {update_part};
                """

                conn.execute(text(upsert_sql))
                conn.execute(text(f"DROP TABLE IF EXISTS {tmp_name};"))
                msg = f"Данные синхронизированы: {len(df)} строк"

        end_ts = datetime.now()
        update_log_query = f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s, message = %s WHERE id = %s;"
        hook.run(update_log_query, parameters=(end_ts, 'Success', f'Успешно загружено {len(df)} строк', log_id))

    except Exception as e:
        end_ts = datetime.now()
        error_msg = str(e)[:500]
        update_log_query = f"UPDATE {SCHEMA_LOGS}.etl_log SET end_ts = %s, status = %s, message = %s WHERE id = %s;"
        hook.run(update_log_query, parameters=(end_ts, 'Error', f'Ошибка: {error_msg}', log_id))
        raise e


with DAG(
        dag_id='load_csv_file',
        start_date=datetime(2026, 4, 1),
        schedule=None,
        catchup=False
) as dag:
    tasks_to_load = [
        'ft_balance_f', 'ft_posting_f', 'md_account_d',
        'md_currency_d', 'md_exchange_rate_d', 'md_ledger_account_s'
    ]

    for table in tasks_to_load:
        PythonOperator(
            task_id=f'load_{table}',
            python_callable=insert_data,
            op_kwargs={
                'file_path': f'/opt/airflow/data/{table}.csv',
                'table_name': table,
                'conn_id': CONNECTION_ID
            }
        )