import logging
import os
from configparser import DEFAULTSECT, NoOptionError, NoSectionError
from dataclasses import dataclass
from pprint import pformat
from textwrap import dedent, indent

import click
from snowflake.connector.errors import ProgrammingError as SnowflakeProgrammingError

from pypelayer import s3_util
from pypelayer.config import Config
from pypelayer.layer import LayerCSV, LayerJSON
from pypelayer.logger import logger
from pypelayer.schema import generate_schema_from_csv, generate_schema_from_json

EXPECTED_FILE_TYPES = ["csv", "json"]


@dataclass
class CLIState:
    dry_run: bool
    verbose: bool
    config: Config


@click.group(name="pypelayer")
@click.option(
    "--verbose", "-v", is_flag=True, default=False, help="Print additional information."
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Do not execute Snowflake queries."
    " AWS connection is still required to fetch data.",
)
@click.option(
    "--profile",
    type=str,
    default=DEFAULTSECT,
    help="Profile section to use from config file.",
)
@click.pass_context
def cli(ctx, verbose: bool, dry_run: bool, profile: str):
    """Pypelayer CLI."""
    if verbose:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARN)

    try:
        config = Config(profile)

    except FileNotFoundError:
        logger.error(f"Error while reading config. Config file not found.")
        raise click.Abort()

    except NoSectionError as error:
        logger.error(f"Error while reading config. Section {error.section} not found.")
        raise click.Abort()

    except NoOptionError as error:
        logger.error(f"Error while reading config. Option {error.option} not found.")
        raise click.Abort()

    ctx.obj = CLIState(dry_run, verbose, config)


@cli.group(name="datasource")
def datasource_group():
    """Datasource operations."""


@datasource_group.command(name="new")
@click.option("--s3-path", required=True, help="Path to files on S3.")
@click.option(
    "--files-scan-limit",
    "-f",
    type=int,
    default=10,
    help="Maximum amount of files to scan for schema detection.",
)
@click.option(
    "--table-name",
    "-t",
    type=str,
    default=None,
    help=dedent(
        """\
        Name to use for Snowflake table. The same name will be used
        to create other Snowflake objects and S3 notifications.
        If not specified, last part of S3 prefix will be used.
        Name is not sanitized or validated, but "-" will be replaced
        with "_"."""
    ),
)
@click.option(
    "--file-type",
    "-ft",
    required=False,
    type=click.Choice(EXPECTED_FILE_TYPES, case_sensitive=False),
    help="Type of files to ingest.",
)
@click.option(
    "--replace",
    "-r",
    is_flag=True,
    default=False,
    help="Replace Snowflake objects if objects with given names exist.",
)
@click.option(
    "--backload",
    "-b",
    is_flag=True,
    default=False,
    help="Load all files from S3 after creating snowflake objects.",
)
@click.option(
    "--column-type",
    "-c",
    default=tuple(),
    type=(str, str),
    multiple=True,
    help="Override automatically detected column type (-c column_name column_type)."
    " Column types and names are not validated before executing query.",
)
@click.pass_context
def datasource_new(
    ctx,
    s3_path,
    files_scan_limit,
    table_name,
    file_type,
    replace,
    backload,
    column_type,
):
    """Create new pipeline."""
    dry_run = ctx.obj.dry_run
    config = ctx.obj.config

    snowflake_credentials = {
        "user": config.snowflake_user,
        "password": config.snowflake_password,
        "account": config.snowflake_account,
        "warehouse": config.snowflake_warehouse,
        "database": config.snowflake_database,
        "schema": config.snowflake_schema,
    }

    s3_bucket_name, key = s3_util.parse_uri(s3_path)
    prefix = os.path.dirname(key) + "/"

    if not s3_bucket_name:
        logger.error("Failed parsing S3 path. Please enter valid S3 URI.")
        raise click.Abort()

    # If table_name wasn't provided, parse it from prefix
    if table_name is None:
        table_name = prefix.split("/")[-2]

        if not table_name:
            logger.error(
                "Couldn't parse table name from S3 path prefix. Please provide table name."
            )
            raise click.Abort()

    # If file_type wasn't provided, prase it from file extensions
    if file_type is None:
        file_type = s3_util.get_file_type_from_extension(
            s3_bucket_name, key, EXPECTED_FILE_TYPES
        )

        if file_type is None:
            logger.error(
                "Automatic file type detection couldn't find a file with expected extension."
            )
            raise click.Abort()

    # Generate schema
    if file_type == "json":
        generate_schema = generate_schema_from_json
    elif file_type == "csv":
        generate_schema = generate_schema_from_csv

    try:
        schema = generate_schema(
            s3_bucket_name, prefix, file_type, files_scan_limit, column_type
        )
        logger.info("Detected schema:\n" + indent(pformat(schema), "  "))
    except FileNotFoundError:
        logger.error("Failed generating schema. No files found.")
        raise click.Abort()

    # Create layer object
    if file_type == "json":
        layer = LayerJSON(
            table_name,
            schema["schema"],
            s3_bucket_name,
            prefix,
            config.snowflake_s3_integration,
            replace,
            snowflake_credentials,
            schema["top_level_array"],
        )
    elif file_type == "csv":
        layer = LayerCSV(
            table_name,
            schema["schema"],
            s3_bucket_name,
            prefix,
            config.snowflake_s3_integration,
            replace,
            snowflake_credentials,
        )

    # Generate/execute queries and attach S3 notifications
    table_ddl = layer.generate_table_ddl()
    logger.info("Table DDL:\n" + indent(table_ddl, "  "))
    if not dry_run:
        logger.info("Executing table DDL query...")
        try:
            layer.execute_query(table_ddl)
        except SnowflakeProgrammingError as error:
            if error.errno == 2002:
                logger.error(f"Table {layer.table_name} already exists.")
                raise click.Abort()
            raise error from None

    stage_ddl = layer.generate_stage_ddl()
    logger.info("Stage DDL:\n" + indent(stage_ddl, "  "))
    if not dry_run:
        logger.info("Executing stage DDL query...")
        try:
            layer.execute_query(stage_ddl)
        except SnowflakeProgrammingError as error:
            if error.errno == 2002:
                logger.error(f"Stage {layer.stage_name} already exists.")
                raise click.Abort()
            raise error from None

    pipe_ddl = layer.generate_pipe_ddl()
    logger.info("Pipe DDL:\n" + indent(pipe_ddl, "  "))
    if not dry_run:
        logger.info("Executing pipe DDL query...")
        try:
            layer.execute_query(pipe_ddl)
        except SnowflakeProgrammingError as error:
            if error.errno == 2002:
                logger.error(f"Snowpipe {layer.pipe_name} already exists.")
                raise click.Abort()
            raise error from None

    if not dry_run:
        logger.info("Attaching S3 notifications...")
        layer.attach_sns_arn()

    if backload:
        backload_query = layer.generate_copy_query()
        logger.info("Backload query:\n" + indent(backload_query, "  "))
        if not dry_run:
            logger.info("Executing backload...")
            layer.execute_query(backload_query)

    logger.info("Done.")
