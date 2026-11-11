import pandas as pd
from psycopg2 import sql
from py_scripts.validation import validate_columns


def xlsx2sql(conn, path, table_name):
    df = pd.read_excel(path, dtype={'terminal_id':str,'passport':str})
    replace_staging(conn, df, table_name)

def csv2sql(conn, path, table_name, date_cols=None):
    df = pd.read_csv(path, sep=';', decimal=',', parse_dates=date_cols, dtype={'card_num':str,'terminal':str})
    replace_staging(conn, df, table_name)


def replace_staging(conn, df, table_name):
    """Очистка и вставка через одно SQLAlchemy Connection без своего commit."""
    validate_columns(df, table_name)
    target = sql.Identifier('dwh', table_name).as_string(conn.connection.driver_connection)
    conn.exec_driver_sql(f'TRUNCATE TABLE {target}')
    df.to_sql(name=table_name, con=conn, schema='dwh', if_exists='append', index=False)


def load_stg_cards(conn):
    with conn.cursor() as cursor:
        cursor.execute('truncate table dwh.stg_cards')
        cursor.execute('''
            insert into dwh.stg_cards (card_num, account, create_dt, update_dt)
            select card_num, account, create_dt, update_dt from bank.cards
        ''')


def load_stg_accounts(conn):
    with conn.cursor() as cursor:
        cursor.execute('truncate table dwh.stg_accounts')
        cursor.execute('''
            insert into dwh.stg_accounts (account, valid_to, client, create_dt, update_dt)
            select account, valid_to, client, create_dt, update_dt from bank.accounts
        ''')


def load_stg_clients(conn):
    with conn.cursor() as cursor:
        cursor.execute('truncate table dwh.stg_clients')
        cursor.execute('''
            insert into dwh.stg_clients (
                client_id, last_name, first_name, patronymic,
                date_of_birth, passport_num, passport_valid_to, phone,
                create_dt, update_dt)
            select
                client_id, last_name, first_name, patronymic,
                date_of_birth, passport_num, passport_valid_to, phone,
                create_dt, update_dt
            from bank.clients
        ''')
