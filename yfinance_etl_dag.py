from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    "owner": "calvin",
    "retries": 0,
    "depends_on_past": False,
}

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()

@task
def return_last_90_days(symbol):
  df = yf.download(
      tickers=symbol,
      period="6mo",
      interval="1d",
      group_by="column",
      auto_adjust=False,
      progress=False
  )
  # Flatten possible MultiIndex columns (e.g., ('Open','AAPL') -> 'Open')
  if isinstance(df.columns, pd.MultiIndex):
      df.columns = df.columns.get_level_values(0)

  # Reformatting
  df.columns.name = None
  df = df.reset_index()
  df = df.rename(columns=str.lower)     # normalize column names

  # Add symbol column
  df["symbol"] = symbol
  df = df.sort_values("date", ascending=False).head(90)
  df = df[["symbol", "date", "open", "high", "low", "close", "volume"]]
  return df

@task
def stock_info_pipeline(symbol: str, records: pd.DataFrame):
    """
    Expects records with columns: ['symbol','date','open','high','low','close','volume']
    Uses the passed-in `symbol` for inserts (ignores records['symbol'] if present).
    """
    con = return_snowflake_conn()
    table = "RAW.lab1_stock_info"

    create_stock_table = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        symbol VARCHAR(10),
        date   DATE,
        open   FLOAT,
        high   FLOAT,
        low    FLOAT,
        close  FLOAT,
        volume INT,
        PRIMARY KEY (symbol, date)
    )
    """

    try:
        con.execute("BEGIN;")
        con.execute(create_stock_table)
        con.execute(f"DELETE FROM {table}")
        row_count = 0
        for d, open, high, low, close, volume in records[
            ["date", "open", "high", "low", "close", "volume"]].itertuples(index=False):
            con.execute(f"""
                INSERT INTO {table} (symbol, date, open, high, low, close, volume)
                VALUES ('{symbol}', '{d}', {open}, {high}, {low}, {close}, {volume})
            """)
            row_count += 1
        con.execute("COMMIT;")
        print(f"Transaction committed. Number of inserted records: {row_count}")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(f"Transaction failed. Error: {e}")
        raise

with DAG(
    dag_id="lab1_yfinance_etl",
    description="Stock price pipeline using yfinance API",
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,          # trigger manually for the lab
    catchup=False,
    default_args=default_args,
    tags=["LAB1", "ETL"]
) as dag:
    symbol = Variable.get('stock_symbol')
    records = return_last_90_days(symbol)
    stock_info_pipeline(symbol, records)