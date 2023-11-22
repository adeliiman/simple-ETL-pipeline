import requests, os
import pandas as pd
import sqlalchemy
from set_logger import get_logger

logger = get_logger(__name__)


postgres_host = os.environ.get('postgres_host')
postgres_database = os.environ.get('postgres_database')
postgres_user = os.environ.get('postgres_user')
postgres_password = os.environ.get('postgres_password')
postgres_port = os.environ.get('postgres_port')

connection_uri = f"postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}"
db_engine = sqlalchemy.create_engine(url=connection_uri)


def extract_data():
    headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',}
    url = "http://cdn.tsetmc.com/api/ClosingPrice/GetMarketMap?market=0&size=1366&sector=0&typeSelected=1&hEven=0"

    try:
        res = requests.get(url=url, headers=headers)
        data = res.json()
        df = pd.DataFrame(data)
        logger.info(f"extract data from api done.")
        return df
    except Exception as e:
        logger.info("No Data From API.")
        logger.exception(f"{e}")


def transform_data(df: pd.DataFrame):
    df.drop(columns=['fontColor', 'fontSize', 'hEven', 'lVal30', 'customLabel'], inplace=True)
    df.columns = ["code", "date", "close", "last", "number_trade", "volume", "value", "yesterday", "name", "sector", "percent", "percent_last", "time", "color"]
    logger.info("clean DataFrame.")
    return df


def load_data(df: pd.DataFrame, db_engine=db_engine):
    res = df.to_sql(name='tse', con=db_engine, if_exists='replace', index=False)
    logger.info(f"load {res} records to postgreSQL.")
    return res


def etl():
    try:
        df = extract_data()
        df = transform_data(df)
        res = load_data(df)
        return res
    except Exception as e:
        logger.exception(f"{e}")


etl()



