"""Неизменяемая идентификация загруженных файлов и состояние архивирования."""
from hashlib import sha256

from py_scripts.validation import DataQualityError


def file_hash(path):
    digest=sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def describe_files(paths, supplemental=False):
    records=[]
    for path in paths:
        digest=file_hash(path)
        # Обычный пакет сохраняет точные имена из ТЗ. Дополнительные пакеты
        # помещаются в отдельные подкаталоги, также с суффиксом .backup.
        archive_name=f'late/{digest}/{path.name}.backup' if supplemental else path.name+'.backup'
        records.append((path.name,digest,archive_name))
    return records


def register_files(conn, records, data_dir, report_date, kind):
    with conn.cursor() as cursor:
        for name,digest,archive_name in records:
            if file_hash(data_dir/name)!=digest:
                raise DataQualityError(f'Файл {name} изменился во время загрузки. День отменён.')
            cursor.execute('''INSERT INTO dwh.meta_files(file_name,sha256,report_dt,load_kind,archive_name)
                VALUES (%s,%s,%s,%s,%s) ON CONFLICT(file_name,sha256) DO NOTHING''',
                (name,digest,report_date,kind,archive_name))
