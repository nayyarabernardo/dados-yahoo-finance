import yfinance
from airflow.decorators import dag, task
from airflow.macros import ds_add
from pathlib import Path
import pendulum
from time import sleep
TICKERS = [
            "CRFB3.SA",
            "ASAI3.SA",
            "AMER3.SA",
            "MGLU3.SA"
]

@task()
def get_history(ticker, ds=None, ds_nodash=None):
    file_path = f"/home/nayyara/Downloads/GitHub/dados-yahoo-finance/stocks/{ticker}/{ticker}_{ds_nodash}.csv"
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    yfinance.Ticker(ticker).history(
        period="1d",
        interval="1h",
        start=ds_add(ds, -1),
        end=ds,
        prepost=True,
    ).to_csv(file_path)
    sleep(10)
@dag(
    schedule_interval = "0 0 * * 2-6",
    start_date = pendulum.datetime(2022, 8, 1, tz="UTC"),
    catchup = True
)
def get_stocks_dag():
    for ticker in TICKERS:
        get_history.override(task_id=ticker)(ticker)

dag = get_stocks_dag() 