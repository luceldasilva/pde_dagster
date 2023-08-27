import json
import pandas as pd
from datetime import datetime
from data_engineering.io.extract import extract_table
from data_engineering.io.load import load_dataframe
from data_engineering.utils import get_lookup_fn, hash_id
from data_engineering.constants import TAX_RATE


def break_down_total(row, name_to_price):
    net = row["amount"] / (1 + TAX_RATE)
    unit_price = name_to_price(row["description"])
    quantity = int(net / unit_price)
    tax = round(row["amount"] * TAX_RATE, 2)

    return unit_price, quantity, tax


def transform_online_transactions(df, products_df=None):
    # Gather prerequisites of the operations
    if products_df is None:
        products_df = extract_table("products")

    name_to_sku = get_lookup_fn(products_df, from_col="name", to_col="sku")
    name_to_price = get_lookup_fn(
        products_df,
        from_col="name",
        to_col="unit_price"
    )
    
    transactions = []

    for i, row in df.iterrows():
        data = json.loads(row["stripe_data"])
        
        if pd.isna(data):
            continue

        unit_price, quantity, tax = break_down_total(data, name_to_price)
        transactions.append(
            {
                # 1. Create unique ID by hashing Stripe transaction ID
                "transaction_id": hash_id(data["id"]),
                # 2. Parse Integer timestamp to Datetime object
                "created_at": datetime.utcfromtimestamp(data["created"]),
                # 3. Add location with constant value 'online'
                "location": "online",
                # 4. Extract product_name from Stripe JSON (named description)
                "product_name": data["description"],
                # 5. Enrich with sku from product_name
                "sku": name_to_sku(data["description"]),
                # 6. Add source with constant value 'online'
                "source": "online",
                # 7. Extract pamynet_method from Stripe JSON (named object)
                "payment_method": data["object"],
                "unit_price": unit_price,
                "quantity": quantity,
                "tax": tax,
                # 9. Extract total from Stripe JSON (named amount)
                "total": data["amount"]
            }
        )

    return pd.DataFrame(transactions)


def process_online_transactions():
    df = extract_table("online_transactions")
    df = transform_online_transactions(df)
    load_dataframe(df)