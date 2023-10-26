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
df = pd.DataFrame(columns=["Country","GDP_USD_billion"])
count = 0

html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

tables = data.find_all('tbody')
print(f'Number of tables: ' + str(len(tables)))
rows = tables[2].find_all('tr')
print(f'Number of rows: ' + str(len(rows)))


for i in range(3, 216):  
    col = rows[i].find_all('td')
    if len(col) != 0:
        country = col[0].find('a').get_text()
        gdp = col[2].contents[0]
        data_dict = {"Country": country,
                     "GDP_USD_billion": gdp}
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df, df1], ignore_index=True)

# print(df)

def convert_to_float(value):
    try:
        number = float(value.replace(",", ""))
        float_value = float(number)
        return round(float_value/1000, 2)  # Round to two decimal places
    except ValueError:
        return np.nan

df['GDP_USD_billion'] = df['GDP_USD_billion'].apply(convert_to_float)

df.to_csv(csv_path)
df.to_json(json_path)

conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()

# Connect to the SQLite database (replace 'your_database.db' with the actual database filename)
conn = sqlite3.connect(db_name)

# Create a cursor object to execute SQL commands
cursor = conn.cursor()

# Define the SQL query to retrieve countries with a GDP greater than 100 billion
query = "SELECT Country FROM Countries_by_GDP WHERE GDP_USD_billion > 100"

# Execute the query
cursor.execute(query)

# Fetch the results
results = cursor.fetchall()

# Close the database connection
conn.close()

# Print the countries with GDP greater than 100,000
for row in results:
    print(row[0])
