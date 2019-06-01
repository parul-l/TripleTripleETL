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
    


def get_bucket_content(bucket_name: str, prefix: str, delimiter: str = '/'):
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
    
    delimeter: `str`
        For 'subfolders', use '/'.
        For all 'files', use ''.

    Returns
    ------
    A list of dictionaries. Each dictionary has key 'Prefix'
    and value equal to the `prefix` + subfolder.
    Example: 'gameposition/season=2015-2016/gameid=0021500663/'

    """
    response = s3client.list_objects(
        Bucket=bucket_name,
        Prefix=prefix,
        Delimiter=delimiter
    )

    if delimiter == '/':
        # returns 'subfolders'
        return response.get('CommonPrefixes')
    elif delimiter == '':
        # returns all 'files'
        return [file['Key'] for file in response['Contents']]


def check_key_exists(bucket: str, key: str, s3client, max_time: int = 0):
    response = s3client.list_objects(Bucket=bucket, Prefix=key).get('Contents')
    time_to_appear = 0
    
    while not response and time_to_appear <= max_time:
        time_increment = 1
        time.sleep(time_increment)
        response = s3client.list_objects(Bucket=bucket, Prefix=key).get('Contents')
        time_to_appear += time_increment
    
    if response:
        logger.info('It took {} seconds for the object to appear'.format(time_to_appear))
        return 1
    else:
        logger.info('Object did not appear in the max time of {} seconds'.format(max_time))
        return 0


def copy_bucket_contents(
        copy_source_keys: list, # list of all files to move
        destination_bucket: str,
        destination_folder: str,
        s3client
):
    for file in copy_source_keys:
        copy_params = {'Bucket': destination_bucket, 'Key': file}
        destination_suffix = '/'.join(file.split('/')[1:]) # removes source_folder
        destination_key = '{}/{}'.format(destination_folder, destination_suffix)

        # copy object in to destination    
        s3client.copy_object(
            Bucket=destination_bucket,
            CopySource=copy_params,
            Key=destination_key
        )

def remove_bucket_contents(
        bucket: str,
        key: str,
        max_time: int,
        s3client
):

    # check object is there 
    is_key_there = check_key_exists(
        bucket=bucket,
        key=key,
        s3client=s3client, 
        max_time=max_time
    )
    if is_key_there:
        # delete object from source
        s3client.delete_object(
            Bucket=destination_bucket,
            Key=destination_key
        )
    else:
        # log the error
        logger.error('{} file did not copy in {} time'.format(key, max_time))

    return is_key_there        

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

