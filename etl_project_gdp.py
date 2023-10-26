import requests
import sqlite3
import pandas as pd
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
row_count_start = 3


for i in range(3, 216):  
    col = rows[row_count_start].find_all('td')
    if len(col) != 0:
        country = col[0].find('a').get_text()
        gdp = col[2].contents[0]
        data_dict = {"Country": country,
                     "GDP_USD_billion": gdp}
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df, df1], ignore_index=True)
        row_count_start += 1

print(df)

df.to_csv(csv_path)
df.to_json(json_path)

conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()
