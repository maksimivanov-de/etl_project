import psycopg2
import os
from dotenv import load_dotenv

from py_scripts.ddl import (
    create_schema,
    # терминалы
    create_dwh_dim_terminals_hist,
    create_stg_terminals,
    create_v_terminals,
    create_new_terminals,
    create_del_terminals,
    create_updated_terminals,
    # чёрный список
    create_dwh_fact_passport_blacklist,
    create_stg_passport_blacklist,
    create_new_passports,
    # транзакции
    create_dwh_fact_transactions,
    create_stg_transactions,
    create_new_transactions,
    # карты
    create_dwh_dim_cards_hist,
    create_stg_cards,
    create_v_cards,
    create_new_cards,
    create_del_cards,
    create_updated_cards,
    # счета
    create_dwh_dim_accounts_hist,
    create_stg_accounts,
    create_v_accounts,
    create_new_accounts,
    create_del_accounts,
    create_updated_accounts,
    # клиенты
    create_dwh_dim_clients_hist,
    create_stg_clients,
    create_v_clients,
    create_new_clients,
    create_del_clients,
    create_updated_clients,
    # витрина и метаданные
    create_rep_fraud,
    create_meta_load)

from py_scripts.load import (
    xlsx2sql,
    csv2sql,
    load_stg_cards,
    load_stg_accounts,
    load_stg_clients)

from py_scripts.db import (
    update_terminals_hist,
    update_passport_blacklist,
    update_transactions,
    update_cards_hist,
    update_accounts_hist,
    update_clients_hist,
    build_rep_fraud,
    get_last_load_date,
    log_load)

from py_scripts.files import archive_file

DATA_DIR = 'data'
ARCHIVE_DIR = 'archive'

load_dotenv()

conn = psycopg2.connect(
    host='localhost',
    database='postgres',
    user=os.getenv('DATABASE_USER'),
    password=os.getenv('DATABASE_PASSWORD'),
    port=5432
)

# 1. создание хранилища
create_schema(conn)

create_dwh_dim_terminals_hist(conn)
create_dwh_fact_passport_blacklist(conn)
create_dwh_fact_transactions(conn)
create_dwh_dim_cards_hist(conn)
create_dwh_dim_accounts_hist(conn)
create_dwh_dim_clients_hist(conn)
create_rep_fraud(conn)
create_meta_load(conn)

create_stg_terminals(conn)
create_stg_passport_blacklist(conn)
create_stg_transactions(conn)
create_stg_cards(conn)
create_stg_accounts(conn)
create_stg_clients(conn)

create_v_terminals(conn)
create_v_cards(conn)
create_v_accounts(conn)
create_v_clients(conn)

# 2. справочники из bank
load_stg_cards(conn)
load_stg_accounts(conn)
load_stg_clients(conn)

create_new_cards(conn)
create_del_cards(conn)
create_updated_cards(conn)

create_new_accounts(conn)
create_del_accounts(conn)
create_updated_accounts(conn)

create_new_clients(conn)
create_del_clients(conn)
create_updated_clients(conn)

update_cards_hist(conn)
update_accounts_hist(conn)
update_clients_hist(conn)

# 3. обработка файлов по дням, от ранней даты к поздней
days = [
    ('01032021', '2021-03-01'),
    ('02032021', '2021-03-02'),
    ('03032021', '2021-03-03'),
]

# день, который уже загружали, пропускаем
last_load = get_last_load_date(conn)
if last_load:
    print(f'последняя загрузка: {last_load}')

for dt_str, report_date in days:

    if last_load and report_date <= str(last_load):
        continue

    terminals_path = os.path.join(DATA_DIR, f'terminals_{dt_str}.xlsx')
    blacklist_path = os.path.join(DATA_DIR, f'passport_blacklist_{dt_str}.xlsx')
    transactions_path = os.path.join(DATA_DIR, f'transactions_{dt_str}.txt')

    if not os.path.exists(transactions_path):
        print(f'файлов за {report_date} нет, пропускаю')
        continue

    print(f'--- обрабатываю {report_date} ---')

    # загрузка в стейджинг
    xlsx2sql(conn, terminals_path, 'stg_terminals')
    xlsx2sql(conn, blacklist_path, 'stg_passport_blacklist')
    csv2sql(conn, transactions_path, 'stg_transactions', ['transaction_date'])

    # расчёт инкрементов
    create_new_terminals(conn)
    create_del_terminals(conn)
    create_updated_terminals(conn)

    create_new_passports(conn)

    create_new_transactions(conn)

    # применение инкрементов
    update_terminals_hist(conn, report_date)
    update_passport_blacklist(conn)
    update_transactions(conn)

    # витрина за этот день
    build_rep_fraud(conn, report_date)

    # отаботанные файлы в архив
    log_load(conn, report_date)
    archive_file(terminals_path, ARCHIVE_DIR)
    archive_file(blacklist_path, ARCHIVE_DIR)
    archive_file(transactions_path, ARCHIVE_DIR)

conn.close()
