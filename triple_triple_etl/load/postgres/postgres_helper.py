import os
import yaml

from triple_triple_etl.constants import DATATABLES_DIR, SCHEMA_DIR
from triple_triple_etl.load.postgres.postgres_connection import get_cursor


def get_primary_keys(tablename, schema_file='position_data_tables.yaml'):
    filename = os.path.join(SCHEMA_DIR, schema_file)

    with open(filename, 'r') as schema_file:
        schema = yaml.load(schema_file)

    def primary_key(c):
        return 'primary key' in c.get('constraints', {})

    columns = schema['tables'][tablename]['columns']
    return map(lambda x: x['name'], filter(primary_key, columns))

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
