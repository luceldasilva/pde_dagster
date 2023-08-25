from dagster import op
from data_engineering.io.drive import parallel_pack


@op(name="parallel_load_files_op")
def parallel_load_files_op(context):
    df = parallel_pack()

    return df