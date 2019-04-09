import contextlib
import psycopg2

from triple_triple_etl.constants import config

@contextlib.contextmanager
def get_connection():
    connection = psycopg2.connect(**config['postgres'])
    connection.set_client_encoding('utf-8')
    connection.autocommit = True

    try:
        yield connection
    finally:
        connection.close()


@contextlib.contextmanager
def get_cursor():
    with get_connection() as connection:
        cursor = connection.cursor()

        try:
            yield cursor
        finally:
            cursor.close()
