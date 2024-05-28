import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name','MC_USD_Billion','MC_GBP_Billion','MC_EUR_Billion','MC_INR_Billion']
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
    for row in tr:
        cell = row.find_all('td')
        if len(cell) != 0:
            dict = {
                'Name': cell[1].text[1:-2],
                'MC_USD_Billion': cell[2].text[:-1]
            }
            temp_df = pd.DataFrame(dict, index=[0])
            df = pd.concat([temp_df,df], ignore_index=True)
    print(df)
    log_progress('Data extraction complete. Initiating Transformation process')
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    log_progress('Data transformation complete. Initiating Loading process')
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    log_progress('Data saved to CSV file')

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    log_progress('Data loaded to Database as a table, Executing queries')

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    log_progress('Process Complete')

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

extract(url,table_attribs)