# TODO: Re-work so that I'm only using boto3.client or boto3.resource
# Currently using both
# TODO: Fix logging output for boto3. Logs output to this file s3.py

import datetime
import logging
import os
import pandas as pd
import re
import tempfile

import boto3
import patoolib

from triple_triple_etl.constants import DATASETS_DIR, META_DIR, LOGS_DIR
from triple_triple_etl.log import get_logger

# THIS_FILENAME = os.path.splitext(os.path.basename(__file__))[0]
# LOG_FILENAME = '{}.log'.format(os.path.splitext(THIS_FILENAME)[0])
# logger = get_logger(output_file=T)
logger = logging.getLogger()
logger.setLevel('INFO')

s3 = boto3.resource('s3')
s3client = boto3.client('s3')
REGEX = re.compile("(.+/rawdata/)(\d.+\d.)([a-zA-Z])")


def get_game_files(bucket_name: str, save_name: str):
    bucket = s3.Bucket(bucket_name)
    
    logger.info('Getting list of .7z files')
    all_files = [
        obj.key for obj in bucket.objects.all()
        if 'Raw' not in obj.key and '.7z' in obj.key
    ]
    df_all_files = pd.DataFrame(data=all_files, columns=['filename'])
    # add additional columns
    df_all_files['season'] = df_all_files.filename.apply(
        lambda x: x.split('/')[0]
    )
    df_all_files['gamedate'] = df_all_files.filename.apply(
        lambda x: datetime.datetime.strptime(REGEX.match(x).group(2), '%m.%d.%Y.')
    )

    # save the data
    df_all_files.to_parquet(os.path.join(META_DIR, save_name))
    

def get_s3_subfolders(bucket_name: str, prefix: str):
    """
    This function returns the elements ('subfolders) in the given `bucket_name`
    with keys beginning with the given `prefix`.

    Parameters
    ----------
    bucket_name: `str`
        The s3 bucket name containing the data
    
    prefix: `str`
        The full key prefix, ending in '/'.
        Example: 'gameposition/season=2015-2016/'

    Returns
    ------
    A list of dictionaries. Each dictionary has key 'Prefix'
    and value equal to the `prefix` + subfolder.
    Example: 'gameposition/season=2015-2016/gameid=0021500663/'

    """
    response = s3client.list_objects(
        Bucket=bucket_name,
        Prefix=prefix,
        Delimiter='/'
    )

    return response.get('CommonPrefixes')


def rename_game_files(bucket_name: str):
    # restructuring "folder" structure
    all_files = get_game_files(bucket_name)
    for file in all_files:
        # create new 'folder' structure
        pieces = file.split('/')
        pieces.insert(1, 'rawdata') # add raw data in middle of pieces list

        # copy old location to new location
        s3.Object(bucket_name, '/'.join(pieces))\
          .copy_from(CopySource=bucket_name + '/' + file)

        # delete old file name
        s3.Object(bucket_name, file).delete()



def s3download(bucket_name: str, filename: str):
    # gettempdir() returns the name of the directory used for temporary files
    logger.info('Creating tmp directory')
    temp_name = filename.replace('/', '.')
    filepath = os.path.join(tempfile.gettempdir(), temp_name)

    try:
        logger.info('Downloading file from s3')
        bucket = s3.Bucket(bucket_name)
        bucket.download_file(Key=filename, Filename=filepath)
        return filepath

    except boto3.exceptions.botocore.client.ClientError as err:
        logger.error(err)
        raise


def extract2dir(filepath: str, directory: str = DATASETS_DIR):
    try:
        logger.info('Unzip file as .json')
        patoolib.extract_archive(filepath, outdir=directory)
        # remove .7z file
        logger.info('Remove .7z file')
        os.remove(filepath)
    except FileNotFoundError as err:
        logger.error(err)
        raise

