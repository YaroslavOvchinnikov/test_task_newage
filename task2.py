import os
import pandas as pd
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor, as_completed
import gspread
from gspread_dataframe import set_with_dataframe

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'file.json'
service_account = gspread.service_account(filename="file1.json")
client = bigquery.Client()
work_sheet = service_account.open("2")
date_pages = ["20170801", "20170731", "20170730"]
query_list = []

executor = ThreadPoolExecutor(3)


for date in date_pages:
    sql_query = f"SELECT visitNumber, visitStartTime, date, geoNetwork " \
                f"FROM `bigquery-public-data.google_analytics_sample.ga_sessions_{date}`"
    query_list.append(executor.submit(client.query(sql_query).result))


df = pd.DataFrame()
for future in as_completed(query_list):
    result = future.result()
    df = df.append(result.to_dataframe())


def change_initial_dataframe():
    global df
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
    df_geoNetwork = df['geoNetwork'].apply(pd.Series)
    df = pd.concat([df, df_geoNetwork], axis=1).drop('geoNetwork', axis=1)
    return df


def manipulation_europe_data():
    europe_07_31_df = df[(df['continent'] == 'Europe') & (df['date'] == '2017-07-31')]
    europe_07_31_df = europe_07_31_df[['visitNumber', 'visitStartTime', 'date', 'subContinent', 'country', 'region']]
    return europe_07_31_df


def manipulation_united_states_data():
    united_states_df = df[(df['country'] == 'United States')]
    united_states_df = united_states_df[['visitNumber', 'visitStartTime', 'date', 'metro']]
    return united_states_df


def manipulation_asia_data():
    asia_df = df[(df['continent'] == 'Asia')]
    asia_df = asia_df[['visitNumber', 'visitStartTime', 'date', 'city']]
    return asia_df


change_initial_dataframe()

manipulations = []
manipulations.append(manipulation_europe_data)
manipulations.append(manipulation_asia_data)
manipulations.append(manipulation_united_states_data)

tasks = []
for manipulation in manipulations:
    tasks.append(executor.submit(manipulation))

# write data
ind = 0
for future in as_completed(tasks):
    res = future.result()
    set_with_dataframe(work_sheet.add_worksheet(str(ind), 100, 100), res, include_column_header=True)
    ind +=1





