from __future__ import annotations

from typing import TYPE_CHECKING, Optional
from urllib.parse import urlparse

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Object as S3Object
else:
    S3Object = object


def get_file_type_from_extension(
    s3_bucket_name: str, prefix: str, extensions: list[str]
) -> Optional[str]:
    """Return first file extension found from the list of extensions."""
    s3_bucket = boto3.resource("s3").Bucket(s3_bucket_name)

    for obj in s3_bucket.objects.filter(Prefix=prefix):
        for ext in extensions:
            if obj.key.endswith("." + ext):
                return ext


def parse_uri(s3_uri: str) -> tuple[str, str]:
    """Extract bucket and key from S3 uri."""
    parsed = urlparse(s3_uri, allow_fragments=False)

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    return bucket, key


def list_non_empty_objects(
    s3_bucket_name: str, prefix: str, suffix: str, limit: int
) -> list[S3Object]:
    """
    Get a list of at most <limit> s3 objects.

    Performs a single request to list a <limit> number of files.
    Results of this request are further filtered on suffix
    and content size to exclude not matching or empty objects.
    """
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(
        Bucket=s3_bucket_name, Prefix=prefix, MaxKeys=limit
    )

    if response.get("Contents") is None:
        return []

    s3_resource = boto3.resource("s3")
    s3_objects = []
    for obj in response["Contents"]:
        if obj["Key"].endswith(suffix) and obj["Size"] > 0:
            s3_objects.append(
                s3_resource.Object(bucket_name=s3_bucket_name, key=obj["Key"])
            )
    return s3_objects
