from data_engineering.io.extract import extract_table
from dagster import op


@op(
    name="extract_partitioned_table_pos",
    required_resource_keys={"postgres_resource"}
)
def extract_table_pos_op(context):
    _, _, pg_conn = context.resources.postgres_resource
    
    start_time = context.op_config.get('start_time')
    end_time = context.op_config.get('end_time')

    df = extract_table(
            "pos_transactions",
            pg_conn=pg_conn,
            start_time=start_time,
            end_time=end_time
        )

    context.log.info(f"Extracted {len(df)} records from pos_transactions")

    return df