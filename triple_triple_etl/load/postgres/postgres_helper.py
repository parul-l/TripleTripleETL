import os

from triple_triple_etl.constants import DATATABLES_DIR
from triple_triple_etl.load.postgres.postgres_connection import get_cursor


def csv2postgres(filepath):
    with get_cursor() as cursor:
        with open(filepath) as f:
            f.readline()
            tablename = os.path.basename(filepath).replace('.csv', '')
            cursor.copy_from(f, tablename, sep=',', null='')


def save_all_tables(tables_dict, storage_dir=DATATABLES_DIR):
    for name, df in tables_dict.items():
        filepath = os.path.join(storage_dir, name + '.csv')
        df.to_csv(filepath, index=False)        
