from __future__ import annotations

from abc import ABC
from textwrap import dedent, indent

import boto3
import snowflake
from snowflake.connector.cursor import SnowflakeCursor


class Layer(ABC):
    """
    Creates Snowflake objects and S3 notifications.

    Does not implement abstract methods, but should be
    inherited from by other Layer classes.
    """

    def __init__(
        self,
        table_name: str,
        table_schema: dict[str, str],
        file_format: str,
        file_pattern: str,
        columns_dml: str,
        s3_bucket_name: str,
        s3_prefix: str,
        storage_integration: str,
        replace: bool = False,
        snowflake_credentials: dict[str, str] = None,
    ):
        table_name = table_name.replace("-", "_")

        self.table_name = table_name
        self.pipe_name = table_name + "_pipe"
        self.stage_name = table_name + "_stage"
        self.s3_trigger_name = table_name + "_snowpipe_trigger"

        self.table_schema = table_schema
        self.file_pattern = file_pattern
        self.file_format = file_format
        self.columns_dml = columns_dml

        self.s3_bucket_name = s3_bucket_name
        self.s3_prefix = s3_prefix
        self.storage_integration = storage_integration
        self.replace = replace

        self.snowflake_credentials = snowflake_credentials
        self._snowflake_connection = None

    def get_cursor(self) -> SnowflakeCursor:
        if self._snowflake_connection is None:
            self._snowflake_connection = snowflake.connector.connect(
                **self.snowflake_credentials
            )

        return self._snowflake_connection.cursor()

    def execute_query(self, query: str) -> list[tuple]:
        with self.get_cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
        return result

    def attach_sns_arn(self) -> None:
        s3_client = boto3.client("s3")

        # Get existing configs
        notification_config = s3_client.get_bucket_notification_configuration(
            Bucket=self.s3_bucket_name
        )

        # Remove existing version of this snowpipe trigger
        notification_config["QueueConfigurations"] = [
            config
            for config in notification_config.get("QueueConfigurations", [])
            if config["Id"] != self.s3_trigger_name
        ]

        # Add new trigger
        notification_config["QueueConfigurations"].append(
            {
                "Id": self.s3_trigger_name,
                "Filter": {
                    "Key": {
                        "FilterRules": [{"Name": "prefix", "Value": self.s3_prefix}]
                    }
                },
                "Events": ["s3:ObjectCreated:*"],
                "QueueArn": self._get_pipe_sns_arn(),
            }
        )

        del notification_config["ResponseMetadata"]

        s3_client.put_bucket_notification_configuration(
            Bucket=self.s3_bucket_name, NotificationConfiguration=notification_config
        )

    def generate_stage_ddl(self) -> str:
        or_replace = " OR REPLACE" if self.replace else ""

        query = dedent(
            """\
            CREATE{or_replace} STAGE {stage_name}
            URL='s3://{s3_bucket_name}/{s3_prefix}'
            STORAGE_INTEGRATION={storage_integration}
            {file_format};"""
        ).format(
            or_replace=or_replace,
            stage_name=self.stage_name,
            s3_bucket_name=self.s3_bucket_name,
            s3_prefix=self.s3_prefix,
            storage_integration=self.storage_integration,
            file_format=self.file_format.strip(),
        )

        return query

    def _get_pipe_sns_arn(self) -> str:
        query = dedent(
            f"""\
            SELECT notification_channel_name
            FROM information_schema.pipes
            WHERE LOWER(pipe_name) = '{self.pipe_name}'
            LIMIT 1;"""
        )

        with self.get_cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()[0]
        return result

    @staticmethod
    def sanitize_column_name(column_name: str) -> str:
        return column_name.replace(".", "_").replace("-", "_")

    def generate_table_ddl(self) -> str:
        or_replace = " OR REPLACE" if self.replace else ""

        columns_definition = ",\n".join(
            f"{self.sanitize_column_name(col_name)} {col_type}"
            for col_name, col_type in self.table_schema.items()
        )

        query = dedent(
            """\
            CREATE{or_replace} TABLE {table_name} (
            {columns_definition}
            );"""
        ).format(
            or_replace=or_replace,
            table_name=self.table_name,
            columns_definition=indent(columns_definition, " "),
        )

        return query

    def generate_pipe_ddl(self) -> str:
        or_replace = " OR REPLACE" if self.replace else ""
        auto_ingest = " AUTO_INGEST = TRUE"

        query = f"CREATE{or_replace} PIPE {self.pipe_name}{auto_ingest} AS {self.generate_copy_query()}"

        return query

    def generate_copy_query(self) -> str:
        columns_names = ",\n".join(
            f"{self.sanitize_column_name(col_name)}"
            for col_name in self.table_schema.keys()
        )

        copy_query = dedent(
            """\
            COPY INTO {table_name} (
            {columns_names}
            ) FROM (
            SELECT
            {columns_dml}
            FROM @{stage_name}
            )
            {file_pattern}
            {file_format};"""
        ).format(
            table_name=self.table_name,
            columns_names=indent(columns_names, "  "),
            columns_dml=indent(self.columns_dml, "    "),
            stage_name=self.stage_name,
            file_pattern=self.file_pattern,
            file_format=self.file_format,
        )

        return copy_query


class LayerCSV(Layer):
    """Wrapper around Layer class for CSV files."""

    def __init__(
        self,
        table_name: str,
        table_schema: dict[str, str],
        s3_bucket_name: str,
        s3_prefix: str,
        storage_integration: str,
        replace: bool = False,
        snowflake_credentials: dict[str, str] = None,
    ):
        file_format = dedent(
            f"""\
            FILE_FORMAT=(
              TYPE='CSV',
              FIELD_OPTIONALLY_ENCLOSED_BY='\"',
              SKIP_HEADER=1,
              NULL_IF=('')
            )"""
        )
        file_pattern = "PATTERN='.*.csv'"
        columns_dml = ",\n".join(
            f"${idx} AS {self.sanitize_column_name(col_name)}"
            for idx, col_name in enumerate(table_schema.keys(), 1)
        )

        super().__init__(
            table_name,
            table_schema,
            file_format,
            file_pattern,
            columns_dml,
            s3_bucket_name,
            s3_prefix,
            storage_integration,
            replace,
            snowflake_credentials,
        )


class LayerJSON(Layer):
    """Wrapper around Layer class for JSON files."""

    def __init__(
        self,
        table_name: str,
        table_schema: dict[str, str],
        s3_bucket_name: str,
        s3_prefix: str,
        storage_integration: str,
        replace: bool = False,
        snowflake_credentials: dict[str, str] = None,
        json_top_level_array: bool = None,
    ):

        file_format = dedent(
            f"""\
            FILE_FORMAT=(
              TYPE='JSON', 
              STRIP_OUTER_ARRAY={json_top_level_array}
            )"""
        )
        file_pattern = "PATTERN='.*.json'"
        columns_dml = ",\n".join(
            f"""$1:"{col_name.replace(".", '":"')}" AS {self.sanitize_column_name(col_name)}"""
            for col_name in table_schema.keys()
        )

        super().__init__(
            table_name,
            table_schema,
            file_format,
            file_pattern,
            columns_dml,
            s3_bucket_name,
            s3_prefix,
            storage_integration,
            replace,
            snowflake_credentials,
        )
