from setuptools import find_packages, setup

setup(
    name="pde_dagster",
    packages=find_packages(exclude=["pde_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "boto3",
        "numpy",
        "pandas",
        "matplotlib",
        "textblob",
        "tweepy",
        "python-decouple",
        "wordcloud",
        "oauth2client",
        "google-api-python-client",
        "psycopg2-binary",
        "scipy",
        "statsmodels",
        "geopy",
        "openpyxl",
        "yfinance",
        "pandas_datareader",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
