"""Блокировка одного запуска на всю базу, включая интервалы между commit."""
from contextlib import contextmanager

# Один и тот же ключ используют ETL и подготовка источника.
LOCK_KEY = (4543564, 1)


@contextmanager
def pipeline_lock(engine):
    # Отдельное подключение удерживает session lock до конца всего запуска.
    # Оно не участвует в транзакциях отдельных дней.
    with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as connection:
        acquired = connection.exec_driver_sql(
            'SELECT pg_try_advisory_lock(%s, %s)', LOCK_KEY
        ).scalar_one()
        if not acquired:
            raise RuntimeError('Для этой базы уже выполняется ETL или настройка. Повторите запуск после завершения.')
        try:
            yield
        finally:
            try:
                connection.exec_driver_sql('SELECT pg_advisory_unlock(%s, %s)', LOCK_KEY)
            except Exception:
                # Session lock нельзя оставлять на подключении, возвращаемом в пул.
                connection.invalidate()
                raise
