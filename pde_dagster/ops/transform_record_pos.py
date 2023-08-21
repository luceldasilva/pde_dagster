import pandas as pd
from dagster import op, In
from data_engineering.process_transactions.pos import transform_pos_transactions


@op(name='transform_record_pos_op', ins={"df": In(pd.DataFrame)})
def transform_record_pos_op(context, df):
    df = transform_pos_transactions(df)
    return df