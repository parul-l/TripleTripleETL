import io
import os
import pandas as pd
import unittest

import boto3
import moto

from triple_triple_etl.core.s3 import (
    get_game_files,
    s3download,
    get_bucket_content,
    check_key_exists,
    copy_bucket_contents,
    remove_bucket_contents
)
from triple_triple_etl.constants import META_DIR

class TestS3(unittest.TestCase):
    """Tests for s3.py"""    
    
    @moto.mock_s3
    def test_s3_get_game_files(self):
    
        s3 = boto3.resource('s3', region_name='us-east-1')

        output_7z = io.BytesIO(b'some contents and stuff')
        output_txt = io.BytesIO(b'some contents and stuff')

        bucket = s3.Bucket('fake-bucket')
        bucket.create()
        bucket.upload_fileobj(
            Fileobj=output_7z,
            Key='somestuff/rawdata/01.01.2016.someotherstuff'
        )
        bucket.upload_fileobj(Fileobj=output_txt, Key='something.txt')

        get_game_files(bucket_name='fake-bucket', save_name='test_get_game_files')
        # check that file exists
        filepath = os.path.join(META_DIR, 'test_get_game_files')
        self.assertTrue(os.path.isfile(filepath))

        # remove the test file
        os.remove(filepath)


    @moto.mock_s3
    def test_s3_get_bucket_content(self):
        s3client = boto3.client('s3')
        s3resource = boto3.resource('s3', region_name='us-east-1')

        # upload contexts to mock bucket
        output_txt = io.BytesIO(b'some contents and stuff')

        bucket = s3resource.Bucket('fake-bucket')
        bucket.create()
        bucket.upload_fileobj(
            Fileobj=output_txt,
            Key='somestuff/testing_contents/some_file.txt'
        )

        contents0 = get_bucket_content(
            bucket_name='fake-bucket',
            prefix='somestuff/',
            delimiter=''
        )
        contents1 = get_bucket_content(
            bucket_name='fake-bucket',
            prefix='somestuff/',
            delimiter=''
        )

        self.assertEqual(
            first=contents0,
            second=['somestuff/testing_contents/some_file.txt'],
            msg=contents0
        )

        self.assertEqual(
            first=contents1,
            second=['somestuff/testing_contents/some_file.txt'],
            msg=contents1
        )

    @moto.mock_s3
    def test_check_key_exists(self):
        s3client = boto3.client('s3')
        s3resource = boto3.resource('s3', region_name='us-east-1')
        # upload contexts to mock bucket
        output_txt = io.BytesIO(b'some contents and stuff')
        key = 'somestuff/testing_contents/some_file.txt'

        bucket = s3resource.Bucket('fake-bucket')
        bucket.create()
        bucket.upload_fileobj(
            Fileobj=output_txt,
            Key=key
        )

        is_file0 = check_key_exists(
            bucket='fake-bucket',
            key=key,
            s3client=s3client,
            max_time=0
        )
        is_file1 = check_key_exists(
            bucket='fake-bucket',
            key='notinbucket',
            s3client=s3client,
            max_time=0
        )
        # file exists
        self.assertEqual(
            first=is_file0,
            second=1
        )
        # file does not exist
        self.assertEqual(
            first=is_file1,
            second=0
        )

    @moto.mock_s3
    def test_copy_bucket_contents(self):
        s3client = boto3.client('s3')
        s3resource = boto3.resource('s3', region_name='us-east-1')
        # upload contexts to mock bucket
        output_txt = io.BytesIO(b'some contents and stuff')
        key = 'somestuff/testing_contents/some_file.txt'

        bucket = s3resource.Bucket('fake-bucket')
        bucket.create()
        bucket.upload_fileobj(
            Fileobj=output_txt,
            Key=key
        )
        # copy stuff over
        copy_bucket_contents(
            copy_source_keys=[key],
            destination_bucket='fake-bucket',
            destination_folder='new_folder',
            s3client=s3client
        )
        # check contents in new folder
        is_file = check_key_exists(
            bucket='fake-bucket',
            key='new_folder/testing_contents/some_file.txt',
            s3client=s3client,
            max_time=0
        )
        self.assertEqual(first=is_file, second=1)  

    @moto.mock_s3
    def test_remove_bucket_contents(self):
        s3client = boto3.client('s3')
        s3resource = boto3.resource('s3', region_name='us-east-1')
        # upload contexts to mock bucket
        output_txt = io.BytesIO(b'some contents and stuff')
        key = 'somestuff/testing_contents/some_file.txt'

        bucket = s3resource.Bucket('fake-bucket')
        bucket.create()
        bucket.upload_fileobj(
            Fileobj=output_txt,
            Key=key
        )

        # remove file
        _ = remove_bucket_contents(
            bucket='fake-bucket',
            key=key,
            max_time=0,
            s3client=s3client
        )
        # check contents gone
        is_file = check_key_exists(
            bucket='fake-bucket',
            key=key,
            s3client=s3client,
            max_time=0
        )
        self.assertEqual(first=is_file, second=0)  

    @moto.mock_s3
    def test_s3download(self):
        s3 = boto3.resource('s3')

        output = io.BytesIO(b'some contents and stuff')
        bucket = s3.Bucket('fake-bucket')

        bucket.create()
        bucket.upload_fileobj(Fileobj=output, Key='something.7z')

        path = s3download(bucket_name='fake-bucket', filename='something.7z')
        with open(path, 'r') as a_file:
            contents = a_file.read()

        assert contents == 'some contents and stuff'
        if os.path.exists(path):
            os.remove(path)

    def test_extract2dir(self):
        # create a file path
        path = os.path.join(os.path.dirname(__file__), 'extract_dir')
        with self.assertRaises(Exception):
            extract2dir(__file__, path)


if __name__ == '__main__':
    unittest.main()
