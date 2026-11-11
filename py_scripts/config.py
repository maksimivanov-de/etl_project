"""Единые настройки для psycopg2 и pandas/SQLAlchemy."""
import getpass
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy.engine import URL

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / '.env')


def database_settings():
    return {
        'host': os.getenv('DATABASE_HOST', 'localhost'),
        'port': int(os.getenv('DATABASE_PORT', '5432')),
        'dbname': os.getenv('DATABASE_NAME', 'etl_project'),
        'user': os.getenv('DATABASE_USER') or getpass.getuser(),
        'password': os.getenv('DATABASE_PASSWORD') or None,
        'connect_timeout': 5,
    }


def database_url():
    settings = database_settings()
    return URL.create(
        'postgresql+psycopg2',
        username=settings['user'],
        password=settings['password'],
        host=settings['host'],
        port=settings['port'],
        database=settings['dbname'],
    )
