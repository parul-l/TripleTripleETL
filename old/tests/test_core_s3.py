import io
import os
import unittest

import boto3
import moto

from triple_triple_etl.core.s3 import get_game_files, s3download


class TestS3(unittest.TestCase):
    """Tests for s3.py"""
    @moto.mock_s3
    def test_s3_get_game_files(self):
        s3 = boto3.resource('s3')

        output_7z = io.BytesIO(b'some contents and stuff')
        output_txt = io.BytesIO(b'some contents and stuff')
        bucket = s3.Bucket('fake-bucket')

        bucket.create()
        bucket.upload_fileobj(Fileobj=output_7z, Key='something.7z')
        bucket.upload_fileobj(Fileobj=output_txt, Key='something.txt')

        assert (get_game_files(bucket_name='fake-bucket') == ['something.7z'])

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
