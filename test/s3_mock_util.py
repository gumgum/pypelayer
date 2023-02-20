import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
else:
    S3Client = object


def get_local_s3_root() -> str:
    return os.path.join(os.path.dirname(__file__), "mock", "s3")


def build_s3(s3_client: S3Client, local_root: str) -> None:
    for bucket in os.listdir(local_root):
        s3_client.create_bucket(Bucket=bucket)

        local_bucket_root = os.path.join(local_root, bucket)

        paths = [
            os.path.join(path, filename)
            for path, _, files in os.walk(local_bucket_root)
            for filename in files
        ]
        for path in paths:
            key = os.path.relpath(path, local_bucket_root)
            key = "/".join(key.split(os.sep))
            s3_client.upload_file(Filename=path, Bucket=bucket, Key=key)
