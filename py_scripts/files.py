from pathlib import Path
from py_scripts.batches import file_hash


def archive_file(path, archive_dir, expected_hash=None, archive_name=None):
    path = Path(path)
    root=Path(archive_dir).resolve()
    new_path = root / (archive_name or path.name + '.backup')
    if not new_path.resolve().is_relative_to(root):
        raise ValueError('Путь архива выходит за пределы каталога archive.')
    expected_hash=expected_hash or file_hash(path)
    if new_path.exists():
        if file_hash(new_path)!=expected_hash:
            raise FileExistsError(f'Архив существует с другим содержимым: {new_path.name}')
        # Если архивирование успело завершиться до сбоя отметки в БД,
        # повтор не должен ни перезаписывать архив, ни удалять новый пакет.
        if path.exists() and file_hash(path)==expected_hash:
            path.unlink()
        return
    if not path.is_file() or file_hash(path)!=expected_hash:
        raise OSError(f'Не найден исходный файл с ожидаемым содержимым: {path.name}')
    new_path.parent.mkdir(parents=True,exist_ok=True)
    path.rename(new_path)
