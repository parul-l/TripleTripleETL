import os
import tempfile

import boto3
import patoolib     # pip install patool

from triple_triple_etl.constants import DATASETS_DIR

s3 = boto3.resource('s3')


def get_game_files(bucket_name):
    bucket = s3.Bucket(bucket_name)
    return [object.key for object in bucket.objects.all()
            if 'Raw' not in object.key and '.7z' in object.key]


def s3download(bucket_name, filename):
    temp_name = filename.replace('/', '.')
    filepath = os.path.join(tempfile.gettempdir(), temp_name)

    try:
        bucket = s3.Bucket(bucket_name)
        bucket.download_file(Key=filename, Filename=filepath)

        return filepath
    except boto3.exceptions.botocore.client.ClientError:
        pass


def extract2dir(filepath, directory=DATASETS_DIR):
    # unzip file as .json
    patoolib.extract_archive(filepath, outdir=directory)
    # remove .7z file
    os.remove(filepath)
