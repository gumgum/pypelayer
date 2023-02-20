import unittest
from test import s3_mock_util

import boto3
from moto import mock_s3

from pypelayer.schema import generate_schema_from_csv, generate_schema_from_json


@mock_s3
class TestSchemaJSON(unittest.TestCase):
    def setUp(self):
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")

        self.local_root = s3_mock_util.get_local_s3_root()
        self.bucket = "json_data"

        s3_mock_util.build_s3(self.s3_client, self.local_root)

    def test_json_no_file(self):
        with self.assertRaises(FileNotFoundError):
            generate_schema_from_json(self.bucket, "non_existing_prefix", "json", 10)

    def test_json_single_file(self):
        result = generate_schema_from_json(self.bucket, "single_file", "json", 10)
        expected = {
            "schema": {
                "column_1": "VARCHAR",
                "column_2": "NUMBER(38,0)",
                "column_3": "NUMBER(38,8)",
                "column_4": "TIMESTAMP WITHOUT TIME ZONE",
                "column_5": "TIMESTAMP WITHOUT TIME ZONE",
                "column_6": "TIMESTAMP WITHOUT TIME ZONE",
                "column_7": "TIMESTAMP WITHOUT TIME ZONE",
                "column_8.other_metric.name": "VARCHAR",
                "column_8.other_metric.value": "NUMBER(38,8)",
                "column_8.some_array": "VARIANT",
                "column_8.nested.a.b": "VARCHAR",
            },
            "top_level_array": False,
        }
        self.assertEqual(result, expected)

    def test_json_single_file_override(self):
        result = generate_schema_from_json(
            self.bucket,
            "single_file",
            "json",
            10,
            (("column_8.nested.a.b", "VARIANT"),),
        )
        expected = {
            "schema": {
                "column_1": "VARCHAR",
                "column_2": "NUMBER(38,0)",
                "column_3": "NUMBER(38,8)",
                "column_4": "TIMESTAMP WITHOUT TIME ZONE",
                "column_5": "TIMESTAMP WITHOUT TIME ZONE",
                "column_6": "TIMESTAMP WITHOUT TIME ZONE",
                "column_7": "TIMESTAMP WITHOUT TIME ZONE",
                "column_8.other_metric.name": "VARCHAR",
                "column_8.other_metric.value": "NUMBER(38,8)",
                "column_8.some_array": "VARIANT",
                "column_8.nested.a.b": "VARIANT",
            },
            "top_level_array": False,
        }
        self.assertEqual(result, expected)

    def test_json_multiple_files(self):
        result = generate_schema_from_json(self.bucket, "multiple_files", "json", 10)
        expected = {
            "schema": {
                "column_1": "VARCHAR",
                "column_2": "NUMBER(38,0)",
                "column_3": "NUMBER(38,8)",
                "column_4": "TIMESTAMP WITHOUT TIME ZONE",
                "column_5": "TIMESTAMP WITHOUT TIME ZONE",
                "column_6": "TIMESTAMP WITHOUT TIME ZONE",
                "column_7": "TIMESTAMP WITHOUT TIME ZONE",
                "column_8.other_metric.name": "VARCHAR",
                "column_8.other_metric.value": "NUMBER(38,8)",
                "column_8.some_array": "VARIANT",
                "column_8.nested.a.b": "VARCHAR",
            },
            "top_level_array": False,
        }
        self.assertEqual(result, expected)

    def test_json_top_level_array_single_file(self):
        result = generate_schema_from_json(
            self.bucket, "top_level_array/single_file", "json", 10
        )
        expected = {
            "schema": {
                "metadata.timestamp": "TIMESTAMP WITHOUT TIME ZONE",
                "metadata.source": "VARCHAR",
                "metadata.owner": "VARCHAR",
                "other_metric.name": "VARCHAR",
                "other_metric.value": "NUMBER(38,8)",
                "metric.name": "VARCHAR",
                "metric.value": "NUMBER(38,0)",
            },
            "top_level_array": True,
        }
        self.assertEqual(result, expected)

    def test_json_top_level_array_multiple_files(self):
        result = generate_schema_from_json(
            self.bucket, "top_level_array/multiple_files", "json", 10
        )
        expected = {
            "schema": {
                "some_array": "VARIANT",
                "metadata.timestamp": "TIMESTAMP WITHOUT TIME ZONE",
                "metadata.source": "VARCHAR",
                "metadata.owner": "VARCHAR",
                "metric.name": "VARCHAR",
                "metric.value": "NUMBER(38,0)",
                "other_metric.name": "VARCHAR",
                "other_metric.value": "NUMBER(38,8)",
                "nested.a.b": "VARCHAR",
            },
            "top_level_array": True,
        }
        self.assertEqual(result, expected)

    def test_json_top_level_array_mixed_files(self):
        with self.assertRaises(ValueError):
            generate_schema_from_json(
                self.bucket, "top_level_array/mixed_files", "json", 10
            )


@mock_s3
class TestSchemaCSV(unittest.TestCase):
    def setUp(self):
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")

        self.local_root = s3_mock_util.get_local_s3_root()
        self.bucket = "csv_data"

        s3_mock_util.build_s3(self.s3_client, self.local_root)

    def test_csv_no_file(self):
        with self.assertRaises(FileNotFoundError):
            generate_schema_from_csv(self.bucket, "non_existing_prefix", "csv", 10)

    def test_csv_single_file(self):
        result = generate_schema_from_csv(self.bucket, "single_file", "csv", 10)
        expected = {
            "schema": {
                "column_1": "BOOLEAN",
                "column_2": "NUMBER(38,0)",
                "column_3": "NUMBER(38,8)",
                "column_4": "VARCHAR",
                "column_5": "TIMESTAMP WITHOUT TIME ZONE",
                "column_6": "TIMESTAMP WITHOUT TIME ZONE",
                "column_7": "VARIANT",
                "column_8": "VARIANT",
            }
        }
        self.assertEqual(result, expected)

    def test_csv_single_file_override(self):
        result = generate_schema_from_csv(
            self.bucket, "single_file", "csv", 10, (("column_1", "VARIANT"),)
        )
        expected = {
            "schema": {
                "column_1": "VARIANT",
                "column_2": "NUMBER(38,0)",
                "column_3": "NUMBER(38,8)",
                "column_4": "VARCHAR",
                "column_5": "TIMESTAMP WITHOUT TIME ZONE",
                "column_6": "TIMESTAMP WITHOUT TIME ZONE",
                "column_7": "VARIANT",
                "column_8": "VARIANT",
            }
        }
        self.assertEqual(result, expected)

    def test_csv_multiple_files(self):
        result = generate_schema_from_csv(self.bucket, "multiple_files", "csv", 10)
        expected = {
            "schema": {
                "column_1": "VARCHAR",
                "column_2": "VARCHAR",
                "column_3": "VARCHAR",
                "column_4": "VARCHAR",
                "column_5": "VARIANT",
            }
        }
        self.assertEqual(result, expected)
