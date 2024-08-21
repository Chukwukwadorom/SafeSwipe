import pandas as pd

url = "https://repo.hops.works/master/hopsworks-tutorials/data/card_fraud_data"

def clean_data(df):
    df = df.dropna()
    df = df[~df.duplicated()]
    return df

def data_ingestion(file):
     url = "https://repo.hops.works/master/hopsworks-tutorials/data/card_fraud_data"
     return clean_data(pd.read_parquet(url + f"/{file}"))

credit_cards_df = data_ingestion("credit_cards.parquet")
trans_df = data_ingestion("transactions.parquet")
profiles_df = data_ingestion("profiles.parquet")

def feature_engineering():
     pass