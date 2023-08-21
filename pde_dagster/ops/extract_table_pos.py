from data_engineering.io.extract import extract_table
from pde_dagster.register.logger import logger
from dagster import op


@op(name="extract_table_pos_op", required_resource_keys={"postgres_resource"})
def extract_table_pos_op(context):
    _, _, pg_conn = context.resources.postgres_resource

    df = extract_table("pos_transactions", pg_conn=pg_conn)

    context.log.info(f"Extracted {len(df)} records from pos_transactions")

    return df