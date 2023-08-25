import pandas as pd
from dagster import op, In

from data_engineering.process_transactions.market import (
    transform_market_transactions
)


@op(name="transform_market_transactions_op", ins={"df": In(pd.DataFrame)})
def transform_market_transactions_op(context, df):
    df, errors = transform_market_transactions(df)
    return df