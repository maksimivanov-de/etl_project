import os


def archive_file(path, archive_dir):
    name = os.path.basename(path)
    new_path = os.path.join(archive_dir, name + '.backup')
    if os.path.isfile(path):
        os.replace(path, new_path)
