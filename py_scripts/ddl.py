import psycopg2

def create_schema(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('create schema if not exists dwh')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False

def create_dwh_dim_terminals_hist(conn): 
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.dwh_dim_terminals_hist (
                    id serial primary key,
                    terminal_id varchar(128),
                    terminal_type varchar(128),
                    terminal_city varchar(128),
                    terminal_address varchar(128),
                    effective_from timestamp default current_timestamp,
                    effective_to timestamp default '2999-12-31 23:59:59',
                    deleted_flg int default 0 not null
                )
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False

def create_dwh_fact_passport_blacklist(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.dwh_fact_passport_blacklist (
                    id serial primary key,
                    passport_num varchar(128),
                    entry_dt date,
                    create_dt timestamp default current_timestamp
                )
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False

def create_stg_terminals(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.stg_terminals (
                    terminal_id varchar(128),
                    terminal_type varchar(128),
                    terminal_city varchar(128),
                    terminal_address varchar(128)
                    )
                ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False



def create_v_terminals(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop view if exists dwh.v_terminals')
            cursor.execute('''
                create view dwh.v_terminals as
                    select
                        terminal_id,
                        terminal_type,
                        terminal_city,
                        terminal_address
                    from dwh.dwh_dim_terminals_hist
                    where deleted_flg = 0
                    and current_timestamp between effective_from and effective_to
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_new_terminals(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_new_terminals')
            cursor.execute('''
                create table dwh.stg_new_terminals as
                    select
                        t1.terminal_id,
                        t1.terminal_type,
                        t1.terminal_city,
                        t1.terminal_address
                    from dwh.stg_terminals t1
                    left join dwh.v_terminals t2 on t1.terminal_id = t2.terminal_id
                    where t2.terminal_id is null
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False
    


def create_del_terminals(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_del_terminals')
            cursor.execute('''
                create table dwh.stg_del_terminals as
                    select
                        t1.terminal_id,
                        t1.terminal_type,
                        t1.terminal_city,
                        t1.terminal_address
                    from dwh.v_terminals t1
                    left join dwh.stg_terminals t2 on t1.terminal_id = t2.terminal_id
                    where t2.terminal_id is null
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_updated_terminals(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_updated_terminals')
            cursor.execute('''
                create table dwh.stg_updated_terminals as
                    select
                        t1.terminal_id,
                        t1.terminal_type,
                        t1.terminal_city,
                        t1.terminal_address
                    from dwh.stg_terminals t1
                    inner join dwh.v_terminals t2 on t1.terminal_id = t2.terminal_id
                    where t1.terminal_city is distinct from t2.terminal_city
                    or t1.terminal_type is distinct from t2.terminal_type
                    or t1.terminal_address is distinct from t2.terminal_address
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_stg_passport_blacklist(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.stg_passport_blacklist(
                    date date,
                    passport varchar(128)
                    )
                ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False 


def create_new_passports(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_new_passports')
            cursor.execute('''
                create table dwh.stg_new_passports as
                    select
                        t1.date,
                        t1.passport
                    from dwh.stg_passport_blacklist t1
                    left join dwh.dwh_fact_passport_blacklist t2
                    on t1.passport = t2.passport_num
                    where t2.passport_num is null
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_stg_transactions(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.stg_transactions (
                    transaction_id bigint,
                    transaction_date timestamp,
                    amount numeric(10, 2),
                    card_num varchar(128),
                    oper_type varchar(128),
                    oper_result varchar(128),
                    terminal varchar(128)
                )
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_dwh_fact_transactions(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.dwh_fact_transactions (
                    id serial primary key,
                    trans_id varchar(128),
                    trans_date timestamp,
                    card_num varchar(128),
                    oper_type varchar(128),
                    amt numeric(10, 2),
                    oper_result varchar(128),
                    terminal varchar(128),
                    create_dt timestamp default current_timestamp
                )
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_new_transactions(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_new_transactions')
            cursor.execute('''
                create table dwh.stg_new_transactions as
                    select
                        t1.transaction_id,
                        t1.transaction_date,
                        t1.amount,
                        t1.card_num,
                        t1.oper_type,
                        t1.oper_result,
                        t1.terminal
                    from dwh.stg_transactions t1
                    left join dwh.dwh_fact_transactions t2
                    on t1.transaction_id::varchar = t2.trans_id
                    where t2.trans_id is null
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_stg_cards(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.stg_cards(
                    card_num varchar(128),
                    account varchar(128),
                    create_dt date,
                    update_dt date)
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_dwh_dim_cards_hist(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.dwh_dim_cards_hist(
                    id serial primary key,
                    card_num varchar(128),
                    account_num varchar(128),
                    effective_from timestamp default current_timestamp,
                    effective_to timestamp default '2999-12-31 23:59:59',
                    deleted_flg int default 0 not null)
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_v_cards(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop view if exists dwh.v_cards')
            cursor.execute('''
                create view dwh.v_cards as
                    select
                        card_num, account_num
                    from dwh.dwh_dim_cards_hist
                    where deleted_flg = 0
                    and current_timestamp between effective_from and effective_to
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_new_cards(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_new_cards')
            cursor.execute('''
                create table dwh.stg_new_cards as
                    select
                        t1.card_num,
                        t1.account as account_num,
                        t1.create_dt,
                        t1.update_dt
                    from dwh.stg_cards t1
                    left join dwh.v_cards t2 on t1.card_num = t2.card_num
                    where t2.card_num is null
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False
    


def create_del_cards(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_del_cards')
            cursor.execute('''
                create table dwh.stg_del_cards as
                    select
                        t1.card_num,
                        t1.account_num
                    from dwh.v_cards t1
                    left join dwh.stg_cards t2 on t1.card_num = t2.card_num
                    where t2.card_num is null
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_updated_cards(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_updated_cards')
            cursor.execute('''
                create table dwh.stg_updated_cards as
                    select
                        t1.card_num,
                        t1.account as account_num,
                        t1.create_dt,
                        t1.update_dt
                    from dwh.stg_cards t1
                    inner join dwh.v_cards t2 on t1.card_num = t2.card_num
                    where t1.account is distinct from t2.account_num
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_stg_accounts(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.stg_accounts(
                    account varchar(128),
                    valid_to date,
                    client varchar(128),
                    create_dt date,
                    update_dt date)
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_dwh_dim_accounts_hist(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.dwh_dim_accounts_hist(
                    id serial primary key,
                    account_num varchar(128),
                    valid_to date,
                    client varchar(128),
                    effective_from timestamp default current_timestamp,
                    effective_to timestamp default '2999-12-31 23:59:59',
                    deleted_flg int default 0 not null)
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_v_accounts(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop view if exists dwh.v_accounts')
            cursor.execute('''
                create view dwh.v_accounts as
                    select
                        account_num, valid_to, client
                    from dwh.dwh_dim_accounts_hist
                    where deleted_flg = 0
                    and current_timestamp between effective_from and effective_to
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_new_accounts(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_new_accounts')
            cursor.execute('''
                create table dwh.stg_new_accounts as
                    select
                        t1.account as account_num,
                        t1.valid_to,
                        t1.client,
                        t1.create_dt,
                        t1.update_dt
                    from dwh.stg_accounts t1
                    left join dwh.v_accounts t2 on t1.account = t2.account_num
                    where t2.account_num is null
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False
    


def create_del_accounts(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_del_accounts')
            cursor.execute('''
                create table dwh.stg_del_accounts as
                    select
                        t1.account_num,
                        t1.valid_to,
                        t1.client
                    from dwh.v_accounts t1
                    left join dwh.stg_accounts t2 on t1.account_num = t2.account
                    where t2.account is null
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_updated_accounts(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_updated_accounts')
            cursor.execute('''
                create table dwh.stg_updated_accounts as
                    select
                        t1.account as account_num,
                        t1.valid_to,
                        t1.client,
                        t1.create_dt,
                        t1.update_dt
                    from dwh.stg_accounts t1
                    inner join dwh.v_accounts t2 on t1.account = t2.account_num
                    where t1.client is distinct from t2.client
                    or t1.valid_to is distinct from t2.valid_to
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_stg_clients(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.stg_clients(
                    client_id varchar(128),
                    last_name varchar(128),
                    first_name varchar(128),
                    patronymic varchar(128),
                    date_of_birth date,
                    passport_num varchar(128),
                    passport_valid_to date,
                    phone varchar(128),
                    create_dt date,
                    update_dt date)
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_dwh_dim_clients_hist(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.dwh_dim_clients_hist(
                    id serial primary key,
                    client_id varchar(128),
                    last_name varchar(128),
                    first_name varchar(128),
                    patrinymic varchar(128),
                    date_of_birth date,
                    passport_num varchar(128),
                    passport_valid_to date,
                    phone varchar(128),
                    effective_from timestamp default current_timestamp,
                    effective_to timestamp default '2999-12-31 23:59:59',
                    deleted_flg int default 0 not null)
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_v_clients(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop view if exists dwh.v_clients')
            cursor.execute('''
                create view dwh.v_clients as
                    select
                        client_id,
                        last_name,
                        first_name,
                        patrinymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone
                    from dwh.dwh_dim_clients_hist
                    where deleted_flg = 0
                    and current_timestamp between effective_from and effective_to
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_new_clients(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_new_clients')
            cursor.execute('''
                create table dwh.stg_new_clients as
                    select
                        t1.client_id,
                        t1.last_name,
                        t1.first_name,
                        t1.patronymic as patrinymic,
                        t1.date_of_birth,
                        t1.passport_num,
                        t1.passport_valid_to,
                        t1.phone,
                        t1.create_dt,
                        t1.update_dt
                    from dwh.stg_clients t1
                    left join dwh.v_clients t2 on t1.client_id = t2.client_id
                    where t2.client_id is null
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_del_clients(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_del_clients')
            cursor.execute('''
                create table dwh.stg_del_clients as
                    select
                        t1.client_id,
                        t1.last_name,
                        t1.first_name,
                        t1.patrinymic,
                        t1.date_of_birth,
                        t1.passport_num,
                        t1.passport_valid_to,
                        t1.phone
                    from dwh.v_clients t1
                    left join dwh.stg_clients t2 on t1.client_id = t2.client_id
                    where t2.client_id is null
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_updated_clients(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('drop table if exists dwh.stg_updated_clients')
            cursor.execute('''
                create table dwh.stg_updated_clients as
                    select
                        t1.client_id,
                        t1.last_name,
                        t1.first_name,
                        t1.patronymic as patrinymic,
                        t1.date_of_birth,
                        t1.passport_num,
                        t1.passport_valid_to,
                        t1.phone,
                        t1.create_dt,
                        t1.update_dt
                    from dwh.stg_clients t1
                    inner join dwh.v_clients t2 on t1.client_id = t2.client_id
                    where t1.last_name is distinct from t2.last_name
                    or t1.first_name is distinct from t2.first_name
                    or t1.patronymic is distinct from t2.patrinymic
                    or t1.date_of_birth is distinct from t2.date_of_birth
                    or t1.passport_num is distinct from t2.passport_num
                    or t1.passport_valid_to is distinct from t2.passport_valid_to
                    or t1.phone is distinct from t2.phone
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_rep_fraud(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.rep_fraud(
                    id serial primary key,
                    event_dt timestamp,
                    passport varchar(128),
                    fio varchar(128),
                    phone varchar(128), 
                    event_type varchar(128), 
                    report_dt date)
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def create_meta_load(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                create table if not exists dwh.meta_load(
                    id serial primary key,
                    report_dt date,
                    load_dt timestamp default current_timestamp)
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False
