from decouple import config
import pandas as pd
import yfinance as yf
import pandas_datareader.data as web


yf.pdr_override()


def get_eth_price():
    date_range = pd.read_sql(
        sql='SELECT min(created_at), max(created_at) FROM crypto_transactions'
        con=config('PG_CONN')
    )
    
    eth_price_df = (
        web.get_data_yahoo(
            'ETH-USD',
            eth_range_df.iloc[0]['min'],
            eth_range_df.iloc[0]['max']
        )
    )
    
    return eth_price_df