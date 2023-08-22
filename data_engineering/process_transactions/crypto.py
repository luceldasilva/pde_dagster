from data_engineering.io.extract import extract_table
from data_engineering.io.load import load_dataframe
from data_engineering.io.eth import get_eth_price
from data_engineering.utils import get_lookup_fn, hash_id
from data_engineering.constants import TAX_RATE
import pandas as pd


def transform_crypto_transactions(df, products_df=None):
    if products_df is None:
        products_df = extract_table("products")

    sku_to_name = get_lookup_fn(products_df, from_col="sku", to_col="name")

    eth_price_df = get_eth_price()
    
    transactions = []
    for i, row in df.iterrows():
        # Transfer values that we want to keep unchanged
        transaction = row[
            [
                "transaction_id",
                "created_at",
                "location",
                "sku",
                "payment_method",
                "quantity",
            ]
        ].to_dict()

        transaction["transaction_id"] = hash_id(row["transaction_id"])
        transaction["product_name"] = sku_to_name(row["sku"])
        transaction["source"] = "crypto_sale"

        eth_price = (
            eth_price_df.loc[row['created_at'].strftime('%Y-%m-%d')]['Open']
        )
        usd_total = eth_price * row["total"]
        transaction["total"] = round(usd_total, 2)
        transaction["tax"] = round(usd_total * TAX_RATE, 2)
        transaction["unit_price"] = (
            round(usd_total / (1 + TAX_RATE) / row["quantity"], 2)
        )

        transactions.append(transaction)

    return pd.DataFrame(transactions)


def process_crypto_transactions():
    df = extract_table("crypto_transactions")
    df = transform_crypto_transactions(df)
    load_dataframe(df)