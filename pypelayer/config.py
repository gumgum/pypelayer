from configparser import DEFAULTSECT, ConfigParser, NoSectionError
from pathlib import Path


class Config:
    """Reads contents of pypelayer config file."""

    def __init__(self, profile: str = DEFAULTSECT):
        config_file_path = Path("~", ".pypelayer").expanduser()

        config = ConfigParser()
        result = config.read(config_file_path)

        if not result:
            raise FileNotFoundError(config_file_path)

        if profile != DEFAULTSECT and profile not in config.sections():
            raise NoSectionError(profile)

        self.snowflake_user = config.get(profile, "user")
        self.snowflake_password = config.get(profile, "password")
        self.snowflake_account = config.get(profile, "account")

        self.snowflake_warehouse = config.get(profile, "warehouse")
        self.snowflake_database = config.get(profile, "database")
        self.snowflake_schema = config.get(profile, "schema")

        self.snowflake_s3_integration = config.get(profile, "s3_integration")
