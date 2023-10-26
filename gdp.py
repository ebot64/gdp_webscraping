# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
json_path = 'Countries_by_GDP.json'
csv_path = 'Countries_by_GDP.csv'
count = 0

def extract(url):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=["Country","GDP_USD_billion"])
    tables = data.find_all('tbody')
    # print(f'Number of tables: ' + str(len(tables)))
    rows = tables[2].find_all('tr')
    # print(f'Number of rows: ' + str(len(rows)))

    for i in range(3, len(rows)):  
        col = rows[i].find_all('td')
        if len(col) != 0:
            country = col[0].find('a').get_text()
            gdp = col[2].contents[0]
            data_dict = {"Country": country,
                        "GDP_USD_billion": gdp}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    df['GDP_USD_billion'] = df['GDP_USD_billion'].apply(convert_to_float)

    return df

def convert_to_float(value):
    try:
        number = float(value.replace(",", ""))
        float_value = float(number)
        return round(float_value/1000, 2) 
    except ValueError:
        return np.nan

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    sql_connection.close()

def run_query2(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    
    # Create a cursor object to execute SQL commands
    cursor = sql_connection.cursor()

    # Execute the query
    cursor.execute(query_statement)

    # Fetch the results
    results = cursor.fetchall()

    # Close the database connection
    sql_connection.close()

    # Print the countries with GDP greater than 100,000
    for row in results:
        print(row[0])

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

df = extract(url)
df = transform(df)
load_to_csv(df, csv_path)

sql_connection = sqlite3.connect(db_name)
load_to_db(df, sql_connection, table_name)

# Define the SQL query to retrieve countries with a GDP greater than 100 billion
query_statement = "SELECT * FROM Countries_by_GDP WHERE GDP_USD_billion >= 100"
sql_connection = sqlite3.connect(db_name)
run_query(query_statement, sql_connection)
