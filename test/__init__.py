import os


def del_env(env: str) -> None:
    if os.environ.get(env) is not None:
        del os.environ[env]


del_env("AWS_PROFILE")
del_env("AWS_ACCESS_KEY_ID")
del_env("AWS_SECRET_ACCESS_KEY")
del_env("AWS_SESSION_TOKEN")

from test.test_schema import *
