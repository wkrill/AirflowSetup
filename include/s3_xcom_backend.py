'''
Custom XCOM backend that enables dataframes to be passed between tasks.
This is not possible by default since Dataframes are not json serializable.
For info: https://www.astronomer.io/guides/custom-xcom-backends
'''

from typing import Any
from airflow.models.xcom import BaseXCom
from pandas import DataFrame, read_parquet
import uuid

class S3XComBackend(BaseXCom):
    PREFIX = "s3://"
    BUCKET_NAME = "brejnholt.dk.airflow-xcom-backend"

    @staticmethod
    def serialize_value(value: Any):
        if isinstance(value, DataFrame):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook        = S3Hook()
            key         = "data_" + str(uuid.uuid4())
            filename    = f"{key}.parquet"

            value.to_parquet(filename, compression=None)
            hook.load_file(
                filename=filename,
                key=key,
                bucket_name=S3XComBackend.BUCKET_NAME,
                replace=True
            )
            value = S3XComBackend.PREFIX + key
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result) -> Any:
        result = BaseXCom.deserialize_value(result)
        if isinstance(result, str) and result.startswith(S3XComBackend.PREFIX):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            
            hook     = S3Hook()
            key      = result.replace(S3XComBackend.PREFIX, "")
            filename = hook.download_file(
                key=key,
                bucket_name=S3XComBackend.BUCKET_NAME,
                local_path="/tmp"
            )
            result = read_parquet(filename)
        return result
