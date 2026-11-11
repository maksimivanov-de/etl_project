"""Подготовка отдельной учебной базы без сброса существующих данных."""
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg2
from psycopg2 import sql
from py_scripts.config import database_settings
from py_scripts.locking import LOCK_KEY


def prepare_database():
    settings = database_settings()
    name = settings['dbname']
    if not name or name in {'postgres', 'template0', 'template1'}:
        raise ValueError('Укажите отдельную базу проекта в DATABASE_NAME.')

    admin_settings = {**settings, 'dbname': 'postgres'}
    admin = psycopg2.connect(**admin_settings)
    try:
        admin.autocommit = True
        with admin.cursor() as cursor:
            cursor.execute('SELECT 1 FROM pg_database WHERE datname = %s', (name,))
            if not cursor.fetchone():
                try:
                    cursor.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
                    print(f'Создана база {name}.')
                except psycopg2.errors.DuplicateDatabase:
                    pass  # Другой запуск уже создал базу; далее действует блокировка.
    finally:
        admin.close()

    conn = psycopg2.connect(**settings)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT pg_try_advisory_xact_lock(%s, %s)', LOCK_KEY)
                if not cursor.fetchone()[0]:
                    raise ValueError('Для этой базы уже выполняется ETL или настройка. Повторите после завершения.')
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bank'")
                tables = {row[0] for row in cursor.fetchall()}
                expected = {'cards', 'accounts', 'clients'}
                if expected <= tables:
                    print('Источник bank уже подготовлен. Существующие данные сохранены.')
                elif tables:
                    raise ValueError('В схеме bank есть неполный набор таблиц. Автоматическая загрузка остановлена; существующие данные сохранены.')
                else:
                    cursor.execute((ROOT / 'ddl_dml.sql').read_text(encoding='utf-8'))
                    print('Учебные данные загружены в bank.')
    finally:
        conn.close()


def main():
    env_path = ROOT / '.env'
    # Эксклюзивное создание сохраняет настройки даже при повторном запуске.
    try:
        with env_path.open('x', encoding='utf-8') as target:
            env_path.chmod(0o600)
            with (ROOT / '.env.example').open(encoding='utf-8') as source:
                shutil.copyfileobj(source, target)
        print('Создан .env. Используются локальный PostgreSQL и пользователь ОС.')
    except FileExistsError:
        print('Существующий .env сохранён.')

    # Конфигурация импортирована до создания .env: перечитываем новый шаблон.
    from dotenv import load_dotenv
    load_dotenv(env_path)
    try:
        prepare_database()
    except (psycopg2.Error, ValueError) as error:
        # Не печатаем DSN и потенциальные реквизиты из текста ошибки подключения.
        if isinstance(error, psycopg2.Error):
            print('Не удалось подготовить PostgreSQL. Проверьте запуск сервера, параметры .env и права пользователя на создание базы. SQLSTATE: ' + str(error.pgcode or 'нет'), file=sys.stderr)
        else:
            print(str(error), file=sys.stderr)
        return 1
    print('Настройка завершена. Запуск ETL: .venv/bin/python main.py')
    return 0


if __name__ == '__main__':
    sys.exit(main())
