import os
import tempfile

import boto3
import patoolib

from triple_triple_etl.constants import DATASETS_DIR
from triple_triple_etl.log import get_logger

logger = get_logger()
s3 = boto3.resource('s3')


def get_game_files(bucket_name):
    bucket = s3.Bucket(bucket_name)
    
    logger.info('Getting list of .7z files')
    return [obj.key for obj in bucket.objects.all()
            if 'Raw' not in obj.key and '.7z' in obj.key]


def rename_game_files(bucket_name):
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



def s3download(bucket_name, filename):
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


def extract2dir(filepath, directory=DATASETS_DIR):
    logger.info('Unzip file as .json')
    patoolib.extract_archive(filepath, outdir=directory)
    # remove .7z file
    logger.info('Remove .7z file')
    os.remove(filepath)
