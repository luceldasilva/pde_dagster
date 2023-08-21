'''from dagster import job
from pde_dagster.resources.postgres import postgres_resource
from pde_dagster.ops.extract_table_pos import extract_table_pos_op


@job(resource_defs={"postgres_resource": postgres_resource})
def J1_process_pos_records():
    extract_table_pos_op()
'''

from dagster import job, in_process_executor
from pde_dagster.resources.postgres import postgres_resource
from pde_dagster.ops.extract_table_pos import extract_table_pos_op
from pde_dagster.ops.transform_record_pos import transform_record_pos_op
from pde_dagster.ops.load_table import load_table_op


@job(
    resource_defs={"postgres_resource": postgres_resource},
    executor_def=in_process_executor
)
def J1_process_pos_records():
    df = extract_table_pos_op()
    df = transform_record_pos_op(df)
    load_table_op(df)