import sys
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
import re
from sqlalchemy import create_engine
from py_scripts.config import PROJECT_ROOT, database_url

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
from py_scripts.locking import pipeline_lock
from py_scripts.migrations import apply_migrations
from py_scripts.validation import validate_transactions, validate_file_staging, validate_reference_staging, validate_history, validate_transaction_coverage
from py_scripts.batches import describe_files, register_files, file_hash

DATA_DIR = PROJECT_ROOT / 'data'
ARCHIVE_DIR = PROJECT_ROOT / 'archive'


class ArchiveError(RuntimeError):
    """База уже зафиксирована; не следует сообщать об откате дня."""


def initialize(conn):
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
    apply_migrations(conn)


def refresh_reference_data(conn, report_date):
    load_stg_cards(conn)
    load_stg_accounts(conn)
    load_stg_clients(conn)
    validate_reference_staging(conn)

    create_new_cards(conn)
    create_del_cards(conn)
    create_updated_cards(conn)

    create_new_accounts(conn)
    create_del_accounts(conn)
    create_updated_accounts(conn)

    create_new_clients(conn)
    create_del_clients(conn)
    create_updated_clients(conn)

    update_cards_hist(conn, report_date)
    update_accounts_hist(conn, report_date)
    update_clients_hist(conn, report_date)


def process_day(connection, paths, report_date, refresh_references=False):
    """Все этапы используют одну транзакцию, открытую вызывающим кодом."""
    conn = connection.connection.driver_connection
    if refresh_references:
        refresh_reference_data(conn, report_date)

    terminals_path, blacklist_path, transactions_path = paths
    xlsx2sql(connection, terminals_path, 'stg_terminals')
    xlsx2sql(connection, blacklist_path, 'stg_passport_blacklist')
    csv2sql(connection, transactions_path, 'stg_transactions', ['transaction_date'])
    validate_file_staging(conn, report_date)
    validate_transactions(conn)

    create_new_terminals(conn)
    create_del_terminals(conn)
    create_updated_terminals(conn)
    create_new_passports(conn)
    create_new_transactions(conn)
    update_terminals_hist(conn, report_date)
    update_passport_blacklist(conn)
    update_transactions(conn)
    validate_history(conn)
    validate_transaction_coverage(conn)
    build_rep_fraud(conn, report_date)
    log_load(conn, report_date)


def run_pipeline(engine, days=None, data_dir=DATA_DIR, archive_dir=ARCHIVE_DIR):
    with pipeline_lock(engine):
        _run_pipeline(engine, days, data_dir, archive_dir)


def _run_pipeline(engine, days, data_dir, archive_dir):
    # Создание структуры не меняет бизнес-данные и имеет отдельную транзакцию.
    with engine.begin() as connection:
        conn = connection.connection.driver_connection
        initialize(conn)
        last_load = get_last_load_date(conn)
        with conn.cursor() as cursor:
            cursor.execute('SELECT report_dt::text FROM dwh.meta_load')
            loaded_days={row[0] for row in cursor.fetchall()}
    recover_archives(engine,data_dir,archive_dir)
    if days is None:
        days=discover_days(data_dir)
    if days != sorted(set(days),key=lambda item:item[1]):
        raise ValueError('Даты должны быть уникальными и идти по возрастанию.')
    if last_load:
        print(f'последняя загрузка: {last_load}')

    refresh_references = True
    for dt_str, report_date in days:
        if report_date in loaded_days:
            if any((data_dir/name).exists() for name in [f'transactions_{dt_str}.txt',f'terminals_{dt_str}.xlsx',f'passport_blacklist_{dt_str}.xlsx']):
                raise ValueError(f'День {report_date} уже загружен. Для нового файла транзакций используйте --transactions-only.')
            continue
        if last_load and report_date < str(last_load):
            raise ValueError(f'День {report_date} не был загружен, но более поздние дни уже обработаны. Используйте режим --transactions-only для поздних транзакций.')

        paths = [
            data_dir / f'terminals_{dt_str}.xlsx',
            data_dir / f'passport_blacklist_{dt_str}.xlsx',
            data_dir / f'transactions_{dt_str}.txt',
        ]
        missing = [p.name for p in paths if not p.is_file()]
        if missing:
            raise FileNotFoundError(f'День {report_date}: нет обязательных файлов: {", ".join(missing)}')
        archive_dir.mkdir(parents=True, exist_ok=True)
        records=describe_files(paths)
        for name,digest,archive_name in records:
            target=archive_dir/archive_name
            if target.exists() and file_hash(target)!=digest:
                raise FileExistsError(f'Архив уже содержит {name}.backup с другими данными; день {report_date} не начат.')

        print(f'--- обрабатываю {report_date} ---')
        # SQLAlchemy фиксирует день при выходе и откатывает при любом исключении.
        with engine.begin() as connection:
            process_day(connection, paths, report_date, refresh_references)
            register_files(connection.connection.driver_connection,records,data_dir,report_date,'full')
        refresh_references = False
        recover_archives(engine,data_dir,archive_dir)


def discover_days(data_dir):
    dates=set()
    for path in data_dir.iterdir():
        match=re.fullmatch(r'(?:transactions_(\d{8})\.txt|(?:terminals|passport_blacklist)_(\d{8})\.xlsx)',path.name)
        if match:
            day=datetime.strptime(match.group(1) or match.group(2),'%d%m%Y').date()
            dates.add((day.strftime('%d%m%Y'),day.isoformat()))
    return sorted(dates,key=lambda item:item[1])


def recover_archives(engine,data_dir,archive_dir):
    with engine.connect() as connection:
        pending=connection.exec_driver_sql('SELECT file_name,sha256,archive_name,report_dt FROM dwh.meta_files WHERE NOT archived ORDER BY report_dt,file_name').all()
    for name,digest,archive_name,report_date in pending:
        try:
            archive_file(data_dir/name,archive_dir,expected_hash=digest,archive_name=archive_name)
            with engine.begin() as connection:
                connection.exec_driver_sql('UPDATE dwh.meta_files SET archived=true WHERE file_name=%s AND sha256=%s',(name,digest))
        except Exception as error:
            raise ArchiveError(f'День {report_date} сохранён в БД, но архивирование не завершено. Повторите запуск: он продолжит перенос с проверкой содержимого файлов.') from error


def run_transactions_only(engine,report_date,data_dir=DATA_DIR,archive_dir=ARCHIVE_DIR):
    report_date=date.fromisoformat(report_date)
    with pipeline_lock(engine):
        with engine.begin() as connection:
            conn=connection.connection.driver_connection
            initialize(conn)
            last_load=get_last_load_date(conn)
        recover_archives(engine,data_dir,archive_dir)
        if not last_load or report_date>last_load:
            raise ValueError('Режим дозагрузки предназначен для прошлых дат. Сначала выполните обычную загрузку.')
        path=data_dir/f'transactions_{report_date:%d%m%Y}.txt'
        records=describe_files([path],supplemental=True)
        with engine.begin() as connection:
            conn=connection.connection.driver_connection
            with conn.cursor() as cursor:
                cursor.execute('SELECT archive_name FROM dwh.meta_files WHERE file_name=%s AND sha256=%s',records[0][:2])
                existing=cursor.fetchone()
            if existing:
                # Содержимое уже обработано: сохраняем единственную архивную копию.
                archive_file(path,archive_dir,expected_hash=records[0][1],archive_name=existing[0])
                return
            csv2sql(connection,path,'stg_transactions',['transaction_date'])
            validate_file_staging(conn,report_date,transactions_only=True)
            validate_transactions(conn)
            validate_history(conn)
            validate_transaction_coverage(conn)
            create_new_transactions(conn)
            update_transactions(conn)
            # Изменение последних операций дня может повлиять на окна после полуночи.
            for affected in [report_date, report_date+timedelta(days=1)]:
                build_rep_fraud(conn,affected.isoformat())
            register_files(conn,records,data_dir,report_date,'transactions')
        recover_archives(engine,data_dir,archive_dir)


def main(argv=None):
    parser=argparse.ArgumentParser(description='Ежедневная загрузка и дозагрузка поздних транзакций.')
    parser.add_argument('--date',action='append',type=date.fromisoformat,help='Дата YYYY-MM-DD; можно указать несколько раз.')
    parser.add_argument('--transactions-only',action='store_true',help='Дозагрузить транзакции прошлой даты без изменения справочников.')
    parser.add_argument('--data-dir',type=Path,default=DATA_DIR)
    parser.add_argument('--archive-dir',type=Path,default=ARCHIVE_DIR)
    args=parser.parse_args(argv)
    if args.transactions_only and (not args.date or len(args.date)!=1):
        parser.error('--transactions-only требует ровно одну --date')
    engine = create_engine(database_url())
    try:
        if args.transactions_only:
            run_transactions_only(engine,args.date[0].isoformat(),args.data_dir,args.archive_dir)
        else:
            days=None if args.date is None else sorted([(day.strftime('%d%m%Y'),day.isoformat()) for day in args.date],key=lambda item:item[1])
            run_pipeline(engine,days,args.data_dir,args.archive_dir)
    except Exception as error:
        print(f'[!] Запуск остановлен: {error}', file=sys.stderr)
        return 1
    finally:
        engine.dispose()
    return 0


if __name__ == '__main__':
    sys.exit(main())
