from pde_dagster.register.logger import logger
from decouple import config
import pandas as pd


def extract_table(table_name, pg_conn=None, start_time=None, end_time=None):

    if pg_conn is not None:
        logger.debug('PG_CONN no estaba configurada')
        pg_conn=None

    pg_conn = config("PG_CONN")

    query = f"SELECT * FROM {table_name}"

    if start_time is not None and end_time is not None:
        if table_name == "online_transactions":
            start_timestamp = pd.to_datetime(start_time).timestamp()
            end_timestamp = pd.to_datetime(end_time).timestamp()

            query += (
                f" WHERE stripe_data ->> 'created' >= '{start_timestamp}'" 
                f" AND stripe_data ->> 'created' < '{end_timestamp}'"
            )

        else:
            query += (
                f" WHERE created_at >= '{start_time}'" 
                f" AND created_at < '{end_time}'"
            )

    df = pd.read_sql(
        sql=query,
        con=pg_conn,
    )

    return df