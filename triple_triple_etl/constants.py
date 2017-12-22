import configparser
import os


MODULE_HOME = os.path.dirname(__file__)


config = configparser.ConfigParser()
config.read(os.path.join(MODULE_HOME, 'params.cfg'))


DATATABLES_DIR = os.path.abspath(config['path']['datatables_dir'])
DATASETS_DIR = os.path.abspath(config['path']['datasets_dir'])

MAX_STORAGE_MB = 1000
SCHEMA_DIR = os.path.join(MODULE_HOME, 'load', 'schemata')
