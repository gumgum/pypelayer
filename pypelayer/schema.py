from __future__ import annotations

import json
from textwrap import dedent

import pandas as pd

from pypelayer import s3_util

PANDAS_TO_SNOWFLAKE = {
    "Int64": "NUMBER(38,0)",
    "Float64": "NUMBER(38,8)",
    "string": "VARCHAR",
    "boolean": "BOOLEAN",
    "object": "VARIANT",
    "datetime64[ns]": "TIMESTAMP WITHOUT TIME ZONE",
}


def generate_schema_from_csv(
    s3_bucket_name: str,
    prefix: str,
    suffix: str,
    limit: int,
    override: tuple[str, str] = tuple(),
) -> dict:
    """
    Generate schema from csv files on S3.

    Header is currently required for csv files.
    """
    s3_objects = s3_util.list_non_empty_objects(s3_bucket_name, prefix, suffix, limit)

    if not s3_objects:
        raise FileNotFoundError()

    df = pd.concat(
        [pd.read_csv(obj.get()["Body"], header=0) for obj in s3_objects]
    ).convert_dtypes()

    df = _parse_datetime(df)
    schema = _get_schema_from_df(df)
    schema.update(override)

    return dict(schema=schema)


def generate_schema_from_json(
    s3_bucket_name: str,
    prefix: str,
    suffix: str,
    limit: int,
    override: tuple[str, str] = tuple(),
) -> dict:
    """
    Generate schema from json files on S3.

    Contents of each json file is normalized.
    """
    s3_objects = s3_util.list_non_empty_objects(s3_bucket_name, prefix, suffix, limit)

    if not s3_objects:
        raise FileNotFoundError()

    json_data = (obj.get()["Body"].read().decode("utf-8") for obj in s3_objects)
    json_data = [json.loads(content) for content in json_data]

    is_list = [isinstance(data, list) for data in json_data]

    if all(is_list):
        top_level_array = True
    elif any(is_list):
        files_without_arrays = [
            obj.bucket_name + "/" + obj.key
            for is_array, obj in zip(is_list, s3_objects)
            if not is_array
        ]

        raise ValueError(
            dedent(
                """\
                Top level array has to be present in all files or none of them.
                Files without top level arrays:
                """
                "\n".join(files_without_arrays)
            )
        )
    else:
        top_level_array = False

    df = pd.concat([pd.json_normalize(data) for data in json_data]).convert_dtypes()

    df = _parse_datetime(df)
    schema = _get_schema_from_df(df)
    schema.update(override)

    return dict(schema=schema, top_level_array=top_level_array)


def _parse_datetime(df: pd.Datetime) -> pd.DataFrame:
    """
    Try converting all string columns to datetime.

    Only convert columns formatted as %Y-%m-%d %H:%M:%S
    or similar (e.g. %Y-%m-%d or %Y-%m-%dT%H:%M:%S).
    """
    for column in df.columns:
        if df[column].dtype == "string":
            try:
                df[column] = pd.to_datetime(df[column], format=r"%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass

    return df


def _get_schema_from_df(df: pd.DataFrame) -> dict[str, str]:
    """Translate pandas DataFrame to dict with columns names and Snowflake types."""
    result = {}
    for idx, column in enumerate(df.columns):
        # If column is completely empty, pandas assigns Int64 as dtype
        # Instead use object, as it is the least specific type
        is_empty = len(df) == df[column].isna().sum()
        dtype = "object" if is_empty else str(df.dtypes[idx])

        result[column] = PANDAS_TO_SNOWFLAKE[dtype]

    return result
