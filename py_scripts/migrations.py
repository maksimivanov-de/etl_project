"""Последовательные миграции в транзакции вызывающего кода."""
from py_scripts.config import PROJECT_ROOT


def apply_migrations(conn):
    with conn.cursor() as cursor:
        cursor.execute('''CREATE TABLE IF NOT EXISTS dwh.meta_schema_migrations(
            version text PRIMARY KEY, applied_at timestamp NOT NULL DEFAULT current_timestamp)''')
        for path in sorted((PROJECT_ROOT / 'sql_scripts' / 'migrations').glob('*.sql')):
            cursor.execute('SELECT 1 FROM dwh.meta_schema_migrations WHERE version=%s', (path.name,))
            if cursor.fetchone():
                continue
            # Плохие существующие данные не удаляем: ошибка отменяет миграцию.
            cursor.execute(path.read_text(encoding='utf-8'))
            cursor.execute('INSERT INTO dwh.meta_schema_migrations(version) VALUES (%s)', (path.name,))
