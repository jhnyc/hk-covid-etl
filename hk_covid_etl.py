import os
import time
import pandas as pd
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# track time taken
start = time.time()

# load environment variables
load_dotenv()


def get_database(db_name):
    """Access existing or create new database."""
    connection_string = f"mongodb+srv://{os.getenv('db_username')}:{os.getenv('db_password')}@cluster0.acsxl.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    client = MongoClient(connection_string)
    return client[db_name]


# connect to the database
database = get_database('covid_hk')

# access existing or create collection
building_collection = database['building_list']
case_collection = database['case_details']


# function to get the timestamp which will be passed to the building_url as a parameter
def get_timestamp():
    yesterday = (datetime.today()-timedelta(1)).strftime("%Y%m%d")
    url = f"https://api.data.gov.hk/v1/historical-archive/list-file-versions?url=http%3A%2F%2Fwww.chp.gov.hk%2Ffiles%2Fmisc%2Fbuilding_list_eng.csv&start={yesterday}&end={yesterday}"
    response = requests.get(url).json()
    return response['timestamps'][0]


# extract building list csv
timestamp = get_timestamp()
building_url = f'https://api.data.gov.hk/v1/historical-archive/get-file?url=http%3A%2F%2Fwww.chp.gov.hk%2Ffiles%2Fmisc%2Fbuilding_list_eng.csv&time={timestamp}'
building_list = pd.read_csv(building_url, on_bad_lines='skip')

# extract case details csv
case_url = 'http://www.chp.gov.hk/files/misc/enhanced_sur_covid_19_eng.csv'
case_details = pd.read_csv(case_url, on_bad_lines='skip')


# rename column remove '.' since MongoDB does not work well with field names containing '.'
def rename_columns(df):
    df.columns = [i.replace('.', '') for i in df.columns]
    return df


# split comma-delimited cases into multiple rows
def split_rows(df, column='Related cases', delimiter=','):
    df_ = df.copy()
    df_[column] = df_[column].astype(str).str.split(delimiter)
    return df_.explode(column).reset_index(drop=True)


# convert datetime object
def convert_datetime(df, columns):
    for column in columns:
        df[column] = pd.to_datetime(df[column], errors='coerce')
        df[column] = df[column].astype(
            object).where(df[column].notnull(), None)
    return df


# function to load data into collection
def load_doc(documents, collection):
    try:
        collection.insert_many(documents, ordered=False)
    except BulkWriteError as e:
        pass


# transform building list table
building_list = rename_columns(building_list)
building_list = split_rows(building_list)
building_list = convert_datetime(
    building_list, columns=['Last date of visit of the case(s)'])


# transform confirmed case details table
case_details = rename_columns(case_details)
case_details = convert_datetime(
    case_details, columns=['Report date', 'Date of onset'])


# convert data to list of dictionaries
building_docs = building_list.to_dict('records')
case_docs = case_details.to_dict('records')

# load data
load_doc(building_docs, building_collection)
load_doc(case_docs, case_collection)


# print complete time
print(
    f'ETL of {timestamp.split("-")[0]} data completed in {time.time()-start:.2f}s.')
