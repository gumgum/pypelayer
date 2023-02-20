# pypelayer
A CLI tool to generate end-to-end data ingestion pipelines with Snowflake and AWS.
pypelayer will:
- scan files on given S3 path to find schema
- create Snowflake table, stage and Snowpipe
- attach sns notifications to S3 bucket

Supports csv and json files.

## Setup
AWS connection is configured with environment variables.
Snowflake connection is configured via *.pypelayer* config file in user home directory:
```
[DEFAULT]
user=<user>
password=<password>
account=<account>
warehouse=<warehouse>
database=<database_a>
schema=<schema>
s3_integration=<integration>

[PROFILE_1]
database=<database_b>
```

## Usage
```
Usage: pypelayer [OPTIONS] COMMAND [ARGS]...

  Common settings for other commands.

Options:
  -v, --verbose   Print additional information.
  --dry-run       Print Snowflake queries instead of executing them. AWS
                  connection is still required to fetch data.
  --profile TEXT  Profile section to use from config file.
  --help          Show this message and exit.

Commands:
  datasource  Datasource operations.
```
```
Usage: pypelayer datasource [OPTIONS] COMMAND [ARGS]...

  Datasource operations.

Options:
  --help  Show this message and exit.

Commands:
  new
```
```
Usage: pypelayer datasource new [OPTIONS]

Options:
  --s3-path TEXT                  Path to files on S3.  [required]
  -f, --files-scan-limit INTEGER  Maximum amount of files to scan for schema
                                  detection.
  -t, --table-name TEXT           Name to use for Snowflake table. The same
                                  name will be used to create other Snowflake
                                  objects and S3 notifications. If not
                                  specified, last part of S3 prefix will be
                                  used. Name is not sanitized or validated,
                                  but "-" will be replaced with "_".
  -ft, --file-type [csv|json]     Type of files to ingest.
  -r, --replace                   Replace Snowflake objects if objects with
                                  given names exist.
  -b, --backload                  Load all files from S3 after creating
                                  snowflake objects.
  -c, --column-type <TEXT TEXT>...
                                  Override automatically detected column type
                                  (-c column_name column_type). Column types
                                  and names are not validated before executing
                                  query.
  --help                          Show this message and exit.
```

### Examples
Display Snowflake queries instead of executing them:
```
>>> pypelayer --verbose --dry-run datasource new --s3-path=<S3 URI>
```
Create pipeline and backload existing data. Replace existing objects. Use `<PROFILE>` for Snowflake connection. Display additional information during execution:
```
>>> pypelayer --verbose --profile <PROFILE> datasource new --s3-path=<S3 URI> -b -r
```

## Notes
- pypelayer assumes, that *csv* files have *.csv* extensions and *json* files have *.json* extensions
- if under specified S3 prefix, there are different types of files (e.g. both *csv* and *json* files) pipelayer will use first type it finds
- snowflake credentials shouldn't be necessary to perform a dry run, but S3 access is still needed to scan the dataset

## Development
Build *.whl* with
```
>>> python setup.py bdist_wheel -d build
```
Run tests
```
>>> python -m unittest test
```

### License

[Apache 2.0](https://github.com/gumgum/pypelayer/blob/main/LICENSE.md)

Important Note: This project does not redistribute third party libraries but identifies their availability. The libraries called by this project are subject to their creator licenses. Remember to consult and comply with all licenses in your uses.
