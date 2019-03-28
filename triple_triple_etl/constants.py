import configparser
import os


MODULE_HOME = os.path.dirname(__file__)


config = configparser.ConfigParser()
config.read(os.path.join(MODULE_HOME, 'params.cfg'))


DATATABLES_DIR = os.path.abspath(config['path']['datatables_dir'])
DATASETS_DIR = os.path.abspath(config['path']['datasets_dir'])
LOGS_DIR = os.path.abspath(config['path']['logs_dir'])
META_DIR = os.path.abspath(config['path']['metadata_dir'])
TEST_DIR = os.path.abspath(config['path']['test_dir'])

BASE_URL_GAMELOG = 'https://stats.nba.com/stats/leaguegamelog'
BASE_URL_PLAY = 'http://stats.nba.com/stats/playbyplayv2'
BASE_URL_BOX_SCORE_TRADITIONAL = 'http://stats.nba.com/stats/boxscoretraditionalv2'
BASE_URL_BOX_SCORE_PLAYER_TRACKING = 'http://stats.nba.com/stats/boxscoreplayertrackv2'


MAX_STORAGE_MB = 1000
SCHEMA_DIR = os.path.join(MODULE_HOME, 'load', 'schemata')

# s3 source info
SOURCE_BUCKET = config['s3']['source_bucket']

# s3 destination info
DESTINATION_BUCKET = config['s3']['destination_bucket']
