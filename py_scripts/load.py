import pandas as pd
import os
import psycopg2

def con_str():
    result = 'postgresql://{u}:{p}@localhost:5432/postgres'.format(
        u=os.getenv('DATABASE_USER'),
        p=os.getenv('DATABASE_PASSWORD')
    )
    return result


def xlsx2sql(conn, path, table_name):
    try:
        df = pd.read_excel(path)
        with conn.cursor() as cursor:
            cursor.execute(f'truncate table dwh.{table_name}')
        conn.commit()
        df.to_sql(
            name=table_name,
            if_exists='append',
            index=False,
            schema='dwh',
            con=con_str()
        )
        return True
    except Exception as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False

def csv2sql(conn, path, table_name, date_cols=None):
    try:
        df = pd.read_csv(path, sep=';', decimal=',', parse_dates=date_cols)
        with conn.cursor() as cursor:
            cursor.execute(f'truncate table dwh.{table_name}')
        conn.commit()
        df.to_sql(
            name=table_name,
            if_exists='append',
            index=False,
            schema='dwh',
            con=con_str()
        )
        return True
    except Exception as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def load_stg_cards(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('truncate table dwh.stg_cards')
            cursor.execute('''
                insert into dwh.stg_cards (card_num, account, create_dt, update_dt) 
                select card_num, account, create_dt, update_dt from bank.cards
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def load_stg_accounts(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('truncate table dwh.stg_accounts')
            cursor.execute('''
                insert into dwh.stg_accounts (account, valid_to, client, create_dt, update_dt) 
                select account, valid_to, client, create_dt, update_dt from bank.accounts
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def load_stg_clients(conn):
    try:
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
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False
