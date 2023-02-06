from airflow import DAG
import pendulum
import pandas as pd
import numpy as np
import yfinance as yf
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task
import pendulum
from airflow.macros import ds_add

tickets = ['AMER3.SA', 
           'ASAI3.SA',     
           'CRFB3.SA',  
           'MGLU3.SA']

directories = ['/home/nayyara/Downloads/GitHub/dados-yahoo-finance/AMER3.SA',
               '/home/nayyara/Downloads/GitHub/dados-yahoo-finance/ASAI3.SA',
               '/home/nayyara/Downloads/GitHub/dados-yahoo-finance/CRFB3.SA',
               '/home/nayyara/Downloads/GitHub/dados-yahoo-finance/MGLU3.SA']

def get_stocks(ds, **kwargs): 
    for ticket in tickets:
        if not os.path.exists(ticket):
            os.makedirs(ticket)
        stock_info = yf.Ticker(ticket)
        hist = stock_info.history(period="1d",
            interval="1h",
            start=ds_add(ds, -1),
            end=ds,
            prepost=True)
        filename = f"{ticket}/{ticket}_{str(len(os.listdir(ticket)) + 1).zfill(2)}.csv"
        hist.to_csv(filename)
    return

def concat(**kwargs):
    df_list = []
    for directory in directories:
        all_files = os.listdir(directory)
        csv_files = [file for file in all_files if file.endswith(".csv")]

        for csv_file in csv_files:
            df = pd.read_csv(os.path.join(directory, csv_file))
            ticket = csv_file.split(".")[0]
            df['Ticket'] = ticket
            df_list.append(df)

    merged_df = pd.concat(df_list)
    merged_df.to_csv('dados_geral.csv', index=False)
    return

def transoform(**kwargs):
    merged_df = pd.read_csv('/home/nayyara/Downloads/GitHub/dados-yahoo-finance/dados_geral.csv')
    merged_df['Datetime'] = pd.to_datetime(merged_df['Datetime'])
    merged_df['Data'] = merged_df['Datetime'].dt.strftime('%Y-%m-%d')
    merged_df['Hora'] = merged_df['Datetime'].dt.strftime('%H:%M:%S')
    #merged_df.drop(columns=['Volume', 'Dividends','Stock Splits', 'Date', 'Adj Close','Datetime'], inplace=True)
    def truncate_float(x):
        return '%.3f' % x

    merged_df[['Open','High', 'Low', 'Close']] = merged_df[['Open','High', 'Low', 'Close']].applymap(truncate_float)
    merged_df.to_csv("dados_geral.csv", index=False)
    return

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 8, 1, tz='UTC'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

dag = DAG(
    'get_stocks',
    schedule_interval="0 0 * * 2-6",
    catchup=True,
    default_args=default_args
)

get_stocks_task = PythonOperator(
    task_id='get_stocks',
    python_callable=get_stocks,
    dag=dag
)

concat_task = PythonOperator(
    task_id='concat',
    python_callable=concat,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transoform,
    dag=dag
)

get_stocks_task >> concat_task >> transform_task

