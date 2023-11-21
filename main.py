import requests
import pandas as pd
import logging
import psycopg2
import sqlalchemy


logging.basicConfig(filename="./etl.log", filemode='a', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s' )


def extract_data():
    headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',}
    url = "http://cdn.tsetmc.com/api/ClosingPrice/GetMarketMap?market=0&size=1366&sector=0&typeSelected=1&hEven=0"

    try:
        res = requests.get(url=url, headers=headers)
        data = res.json()
        df = pd.DataFrame(data)
        logging.info(f"extract data from api done.")
        return df
    except Exception as e:
        logging.exception(f"{e}")


def transform_data(df: pd.DataFrame):
    df.drop(columns=['fontColor', 'fontSize', 'hEven'], inplace=True)
    return df


def load_data(df: pd.DataFrame):
    conn = psycopg2.connect(host='127.0.0.1', database='iman', port=5432, user='iman', password='123456')
    cur = conn.cursor()
    query = """ CREATE TABLE IF NOT EXISTS tse_tmc (
    insCode INTEGER,
    dEven INTEGER,
    pClosing FLOAT,
    pDrCotVal FLOAT,
    zTotTran FLOAT,
    lSecVal CHAR(255),
    percent FLOAT,
    priceChangePercent FLOAT,
    hEvenShow CHAR(255),
    color CHAR(255),
    customLabel CHAR(255)
    )"""
    cur.execute(query=query)
    conn.commit()

    connection_uri = "postgresql+psycopg2://iman:123456@localhost:5432/iman"
    db_engine = sqlalchemy.create_engine(url=connection_uri)
    res = df.to_sql(name='tse_tmc', con=db_engine, if_exists='replace', index=False)
    return res


def etl():
    df = extract_data()
    df = transform_data(df)
    load_data(df)