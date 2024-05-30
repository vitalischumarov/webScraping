import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name','MC_USD_Billion','MC_GBP_Billion','MC_EUR_Billion','MC_INR_Billion']
conn = sqlite3.connect('database.sqlite')
table_name = "data"
# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    with open('code_log.txt','a', newline='') as file:
        file.write(f'{datetime.now()}  :  {message} \n')

def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)
    url_data = requests.get(url).text #ULR requets verstenden
    soup = BeautifulSoup(url_data,'html.parser') #Daten in ein sauberes Format umwandeln
    table = soup.find('tbody') #Tabelle auswaehlen
    tr = table.find_all('tr') #alle Zeilen auswaehlen
    for row in tr: #Jede Reihe soll einzelnt ausgewahelt werden
        cell = row.find_all('td') #Alle Daten in einer Zeile auswaehlen
        if len(cell) != 0:
            dict = {
                'Name': cell[1].text[1:-2],
                'MC_USD_Billion': cell[2].text[:-1]
            }
            temp_df = pd.DataFrame(dict, index=[0])
            df = pd.concat([temp_df,df], ignore_index=True)
    #print(df)
    log_progress('Data extraction complete. Initiating Transformation process')
    return df

def transform(df, csv_path):
    exchange_data = pd.read_csv(csv_path)
    for i in range(len(df)):
        df['MC_EUR_Billion'][i] = round(float(df['MC_USD_Billion'][i]) * float(exchange_data['Rate'][0]),2)
        df['MC_GBP_Billion'][i] = round(float(df['MC_USD_Billion'][i]) * float(exchange_data['Rate'][1]),2)
        df['MC_INR_Billion'][i] = round(float(df['MC_USD_Billion'][i]) * float(exchange_data['Rate'][2]),2)
    log_progress('Data transformation complete. Initiating Loading process')
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)
    log_progress('Data saved to CSV file')

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress('Data loaded to Database as a table, Executing queries')

def run_query(query_statement, sql_connection):
    print(pd.read_sql(query_statement, sql_connection))

    log_progress('Process Complete')

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

df_extracted = extract(url,table_attribs)
df_transformed = transform(df_extracted,'./exchange_rate.csv')
load_to_csv(df_transformed,'./data.csv')
load_to_db(df_transformed, conn, table_name)

query = 'SELECT AVG(MC_GBP_Billion) FROM data'

run_query(query, conn)