import pandas as pd
import numpy as np
import os

directories = ['/home/nayyara/Downloads/GitHub/dados-yahoo-finance/stocks/AMER3.SA',
               '/home/nayyara/Downloads/GitHub/dados-yahoo-finance/stocks/ASAI3.SA',               
               '/home/nayyara/Downloads/GitHub/dados-yahoo-finance/stocks/CRFB3.SA',               
               '/home/nayyara/Downloads/GitHub/dados-yahoo-finance/stocks/MGLU3.SA']

def concat():
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

def transoform():
    merged_df = pd.read_csv('/home/nayyara/Downloads/GitHub/dados-yahoo-finance/dados_geral.csv')
    merged_df['Datetime'] = pd.to_datetime(merged_df['Datetime'])
    merged_df['Data'] = merged_df['Datetime'].dt.strftime('%Y-%m-%d')
    merged_df['Hora'] = merged_df['Datetime'].dt.strftime('%H:%M:%S')
    merged_df.drop(columns=['Volume', 'Dividends','Stock Splits', 'Date', 'Adj Close','Datetime'], inplace=True)
    def truncate_float(x):
        return '%.3f' % x

    merged_df[['Open','High', 'Low', 'Close']] = merged_df[['Open','High', 'Low', 'Close']].applymap(truncate_float)
    merged_df.to_csv("dados_geral.csv", index=False)

concat()
transoform()


