import pandas as pd
import glob
import os


#juntar todos Cada csv de cada pasta
def join_AMER3(path):
    all_files = os.listdir(path)
    df_list = []
    for filename in all_files:
        if filename.endswith('.csv'):
            df = pd.read_csv(os.path.join(path, filename))
            df_list.append(df)
    df_all = pd.concat(df_list)
    df_all.to_csv('AMER3.csv')
    print('join 2 pronto')

join_AMER3('/home/nayyara/Downloads/GitHub/dados-yahoo-finance/stocks/AMER3.SA')

def join_CRFB3(path):
    all_files = os.listdir(path)
    df_list = []
    for filename in all_files:
        if filename.endswith('.csv'):
            df = pd.read_csv(os.path.join(path, filename))
            df_list.append(df)
    df_all = pd.concat(df_list)
    df_all.to_csv('CRFB3.csv')
    print('join 2 pronto')

join_CRFB3('/home/nayyara/Downloads/GitHub/dados-yahoo-finance/stocks/CRFB3.SA')


def join_ASAI3(path):
    all_files = os.listdir(path)
    df_list = []
    for filename in all_files:
        if filename.endswith('.csv'):
            df = pd.read_csv(os.path.join(path, filename))
            df_list.append(df)
    df_all = pd.concat(df_list)
    df_all.to_csv('ASAI3.csv')
    print('join 3 pronto')

join_ASAI3('/home/nayyara/Downloads/GitHub/dados-yahoo-finance/stocks/ASAI3.SA')

def join_MGLU3(path):
    all_files = os.listdir(path)
    df_list = []
    for filename in all_files:
        if filename.endswith('.csv'):
            df = pd.read_csv(os.path.join(path, filename))
            df_list.append(df)
    df_all = pd.concat(df_list)
    df_all.to_csv('MGLU3.csv')
    print('join 4 pronto')


join_MGLU3('/home/nayyara/Downloads/GitHub/dados-yahoo-finance/stocks/MGLU3.SA')



# adiciona coluna "ticket" ao csv
def add_AMER3_column():
    AMER3 = pd.read_csv('AMER3.csv')
    AMER3['Ticket'] = 'AMER3'
    AMER3.to_csv('AMER3.csv', index=False)
    print('AMER3 adicionado coluna')
add_AMER3_column()

def add_CRFB3_column():
    CRFB3 = pd.read_csv('CRFB3.csv')
    CRFB3['Ticket'] = 'CRFB3'
    CRFB3.to_csv('CRFB3.csv', index=False)
    print('CRFB3 adicionado coluna')
add_CRFB3_column()

def add_ASAI3_column():
    ASAI3 = pd.read_csv('ASAI3.csv')
    ASAI3['Ticket'] = 'ASAI3'
    ASAI3.to_csv('ASAI3.csv', index=False)
    print('ASAI3 adicionado coluna')
add_ASAI3_column()

def add_MGLU3_column():
    MGLU3 = pd.read_csv('MGLU3.csv')
    MGLU3['Ticket'] = 'MGLU3'
    MGLU3.to_csv('MGLU3.csv', index=False)
    print('MGLU3 adicionado coluna')
add_MGLU3_column()

#juntar todos todos os csv
def concat_csv(csv1, csv2, csv3, csv4):
  df1 = pd.read_csv(csv1)
  df2 = pd.read_csv(csv2)
  df3 = pd.read_csv(csv3)
  df4 = pd.read_csv(csv4)
  df_list = [df1, df2, df3, df4]
  df_concat = pd.concat(df_list)
  df_concat[['Data','Hora']] = df_concat['Datetime'].str.split(expand=True)
  df_concat['Data']= pd.to_datetime(df2['Data'], dayfirst=True)
  df_concat.drop(['Unnamed: 0','Datetime','Adj Close', 'Date', 'Stock Splits', 'Dividends', 'Volume'], axis=1, inplace = True)
  df_concat[['Open', 'High', 'Low', 'Close']] = df2[['Open', 'High', 'Low', 'Close']].round(4)
  df_concat.to_csv('df_concat.csv', index=False)
  print('concatt geral pronto ')
  return df_concat

concat_csv('AMER3.csv', 'CRFB3.csv', 'ASAI3.csv', 'MGLU3.csv')


# if __name__ == "__main__":



