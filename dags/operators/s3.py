import os
import logging
from typing import List, Optional
from pathlib import Path

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_files_to_s3(
        bucket_name: str,
        files=None,
        keys=None,
        dir=None,
        replace=True,
        aws_conn_id='aws_s3'
    ):

    if dir:    # Set keys to preserve folder structure
        logging.info('Received dir')
        files = []
        keys = []
        for (root, dirs, file_names) in os.walk(dir):
            for file in file_names:
                file = Path(root) / file
                files.append(file)
                keys.append(file.relative_to(Path(dir).parent).as_posix())

    elif not keys:
        logging.info('Got no keys')
        keys = [Path(file).name for file in files]

    if len(files) != len(keys):
        raise ValueError(
            f"Number of file paths and keys don't match! "
            f"Got {len(files)} paths and {len(keys)} keys."
        )

    s3_hook =  S3Hook(aws_conn_id=aws_conn_id)
    for file, key in zip(files, keys):
        if replace and s3_hook.check_for_key(key=key, bucket_name=bucket_name):
            logging.info(f'The key {key} already exists. Overwrites file.')
        
        s3_hook.load_file(
            filename=file,
            key=key,
            bucket_name=bucket_name,
            replace=replace,
        )


class UploadFilesToS3Operator(BaseOperator):
    """Upload local files to S3 bucket.

    Args:
        bucket_name (str):
            Name of the bucket to upload to.
        files (List[str], optional):
            Paths to the files to upload.
        keys (List[str], optional):
            S3 keys (i.e names in S3) of the uploaded files.
        dir (str, optional):
            Directory to upload (i.e. set keys to preserve structure).
        replace (bool, optional):
            Whether to replace file in bucket if the key already exists.
            Defaults to True.
        aws_conn_id (str, optional):
            Id of the S3 connection. Defaults to 'aws_s3'.
    """
    template_fields = ('files', 'bucket_name', 'keys')

    color_ui = '#db453c'


    def __init__(
            self,
            *,
            bucket_name: str,
            files: Optional[List[str]] = None,
            keys: Optional[List[str]] = None,
            dir: Optional[str] = None,
            replace: bool = True,
            aws_conn_id: str = 'aws_s3',
            **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.files = files
        self.keys = keys
        self.dir = dir
        self.replace = replace
        self.aws_conn_id = aws_conn_id

    def execute(self, context: dict) -> None:
        upload_files_to_s3(
            bucket_name=self.bucket_name,
            files=self.files,
            keys=self.keys,
            dir=self.dir,
            replace=self.replace,
            aws_conn_id=self.aws_conn_id,
        )
