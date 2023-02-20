import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pypelayer",
    version="0.0.1",
    author="Brendan Frick",
    author_email="brendan@gumgum.com",
    description="Set up Snowpipes quickly",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=["pypelayer"],
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    entry_points={"console_scripts": ["pypelayer=pypelayer.cli:cli"]},
    install_requires=[
        "snowflake-connector-python==2.9.0",
        "pandas==1.5.2",
        "boto3==1.26.35",
        "click==8.1.3",
    ],
)
