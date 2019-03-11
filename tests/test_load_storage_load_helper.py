import os
import unittest

from triple_triple_etl.load.storage.load_helper import get_uploaded_metadata
from triple_triple_etl.constants import META_DIR

class TestUploadedMetaData(unittest.TestCase):
    """Tests for get_uploaded_metadata() from s37z_to_s3parquet.py """
    def test_filepath_exists(self): 
        filename = 'rawpositiontransformed_s3uploaded.parquet.snappy'
        columns = [
            'input_filename',
            'gameinfo_uploadedFLG',
            'gameposition_uploadedFLG',
            'playerinfo_uploadedFLG',
            'teaminfo_uploadedFLG',
            'lastuploadDTS'
        ]
        
        df = get_uploaded_metadata(os.path.join(META_DIR, filename), columns)
        self.assertEqual(
            first=set(df.columns),
            second=set(columns)
        )
    
    def test_filepath_dne(self):
        file_mock = 'somefile'
        columns = [
            'input_filename',
            'gameinfo_uploadedFLG',
            'gameposition_uploadedFLG',
            'playerinfo_uploadedFLG',
            'teaminfo_uploadedFLG',
            'lastuploadDTS'
        ]
        df = get_uploaded_metadata(file_mock, columns)

        # check columns are as expected
        self.assertEqual(
            first=set(df.columns),
            second={
                'input_filename',
                'gameinfo_uploadedFLG',
                'gameposition_uploadedFLG',
                'playerinfo_uploadedFLG',
                'teaminfo_uploadedFLG',
                'lastuploadDTS'
            }
        )
        # check total row count
        self.assertEqual(first=df.shape[0], second=0)


if __name__ == '__main__':
    unittest.main()