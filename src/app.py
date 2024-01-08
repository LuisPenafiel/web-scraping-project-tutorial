#####pip install and requirements.txt ######

#!pip install pandas requests
#pip install -r requirements.txt
#pip install lxml

####### all imports ########
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import sqlite3
import seaborn as sns

####### Web Scrapping #######

####### Web Scrapping #######

#Storing URL
tesla_url = 'https://www.macrotrends.net/stocks/charts/TSLA/tesla/revenue'

#Creating request
time.sleep(20) #20 sec delay before making the request not to overload the server
try:
    response = requests.get(tesla_url)
    response.raise_for_status() #Verify if solicitude was successful
    tesla_raw = BeautifulSoup(response.text, 'html')
#handles errors specific to the request

except requests.exceptions.RequestException as e:
    print(f"Requests Exception: {e}")  #for the error
    header = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"}
    response = requests.get(tesla_url, headers = header)
    tesla_raw = BeautifulSoup(response.text, 'html.parser')

#Prints other exceptions that could happen during parsing
except Exception as e:
    print(f"Other Exception: {e}")

#########finding all tables and storing data as#####
all_tables = tesla_raw.find_all("table")

#finding quarterly evolution table of Tesla's revenue

for table in all_tables:
    if 'Tesla Quarterly Revenue' in table.get_text(): 
        tesla_data = table
        break #stops the code once finds a table

#storing result as dataframe

tesla_qrev = pd.read_html(str(tesla_data))[0]
tesla_qrev.columns = ['quarter', 'revenue']#rename columns
tesla_qrev['quarter'] = pd.to_datetime(tesla_qrev['quarter'])
tesla_qrev['revenue'] = tesla_qrev['revenue'].str.replace('[\$,]', '', regex =True)
tesla_qrev = tesla_qrev.dropna(subset=['revenue']) #remove the NaN!

print(tesla_qrev)

#######exploratory analysis of Tesla's quarter####

### Time serie visualization

fig, axis = plt.subplots(figsize=(10, 5))

tesla_qrev['quarter'] = pd.to_datetime(tesla_qrev['quarter'])
tesla_qrev['revenue'] = tesla_qrev['revenue'].astype('int')
sns.lineplot(data=tesla_qrev, x='quarter', y='revenue')

plt.tight_layout()

plt.show()

### Anual gross benefit

fig, axis = plt.subplots(figsize=(10, 5))

tesla_qrev['quarter'] = pd.to_datetime(tesla_qrev['quarter'])
tesla_qrev_yearly = tesla_qrev.groupby(tesla_qrev['quarter'].dt.year).sum().reset_index()

sns.barplot(data=tesla_qrev_yearly[tesla_qrev_yearly['quarter'] < 2023], x='quarter', y='revenue')

plt.tight_layout()

plt.show()

### Monthly gross benefit

fig, axis = plt.subplots(figsize=(10, 5))

tesla_qrev_monthly = tesla_qrev.groupby(tesla_qrev['quarter'].dt.month).sum().reset_index()

sns.barplot(data=tesla_qrev_monthly, x='quarter', y='revenue')

plt.tight_layout()

plt.show()

###########Creating SQL data base ######
con = sqlite3.connect("tesla_qrev.db")
cursor = con.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS TESLA
    (quarter        DATE    NOT NULL,
     revenue         FLOAT       )""")

tesla_qrev.to_sql('TESLA', con, index = False, if_exists = 'replace') #converting DF to SQl

#Checking if table created properly

for row in cursor.execute("Select * FROM TESLA"):
    print(row)

con.commit()
con.close()
