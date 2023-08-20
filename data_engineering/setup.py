from setuptools import find_packages, setup

setup(
    name="data_engineering",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "pandas_datareader",
        "python-decouple",
        "oauth2client",
        "google-api-python-client",
        "psycopg2-binary",
        "scipy",
        "statsmodels",
        "geopy",
        "openpyxl",
    ],
)