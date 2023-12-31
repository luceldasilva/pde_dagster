from dagster import job, in_process_executor

from pde_dagster.ops.J5_process_files.parallel_load_files import (
    parallel_load_files_op
)
from pde_dagster.ops.J5_process_files.transform_market_transactions import(
    transform_market_transactions_op
)
from pde_dagster.ops.load_table import load_table_op
from pde_dagster.resources.postgres import postgres_resource


@job(
    resource_defs={"postgres_resource": postgres_resource},
    executor_def=in_process_executor
)
def J5_process_files():
    df = parallel_load_files_op()
    df = transform_market_transactions_op(df)
    load_table_op(df)