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


def csv2postgres_pkeys(tablename, filepath, schema_file='position_data_tables.yaml'):
    pkeys_params = {'tablename': tablename, 'schema_file': schema_file}
    pkeys = ','.join(get_primary_keys(**pkeys_params))
    
    query = """
        BEGIN TRANSACTION;
        
        CREATE TABLE {tablename}_staging(LIKE {tablename});
        
        COPY {tablename}_staging
        FROM STDIN
        DELIMITER ',' CSV HEADER;
        
        INSERT INTO {tablename}
          SELECT DISTINCT ON ({primary_key_fields}) *
          FROM {tablename}_staging
        ON CONFLICT ({primary_key_fields}) DO NOTHING;
        
        DROP TABLE {tablename}_staging;
        
        END TRANSACTION;
    """.format(tablename=tablename, primary_key_fields=pkeys)

    with get_cursor() as cursor:
        with open(filepath) as f:
            cursor.copy_expert(query, f)


def csv2postgres_no_pkeys(filepath):
    tablename = os.path.basename(filepath).replace('.csv', '')
    query = """
        BEGIN TRANSACTION;
        
        CREATE TABLE {tablename}_staging(LIKE {tablename});
        
        INSERT INTO {tablename}_staging
          SELECT DISTINCT *
          FROM {tablename};
        
        DROP TABLE {tablename};
        
        ALTER TABLE {tablename}_staging
          RENAME TO {tablename};
        
        END TRANSACTION;
    """.format(tablename=tablename)

    with get_cursor() as cursor:
        with open(filepath) as f:
            f.readline()        
            cursor.copy_from(f, tablename, sep=',', null='')
            cursor.execute(query)

def save_all_tables(tables_dict, storage_dir=DATATABLES_DIR):
    for name, df in tables_dict.items():
        filepath = os.path.join(storage_dir, name + '.csv')
        df.to_csv(filepath, index=False)        
