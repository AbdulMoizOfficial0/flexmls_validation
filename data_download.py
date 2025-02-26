import time
import pandas as pd
import psycopg2
import csv
from psycopg2 import extras
import os
from dotenv import load_dotenv
from pathlib import Path
from rets import Session
from datetime import datetime
import requests
from requests.auth import HTTPDigestAuth
import xml.etree.ElementTree as ET

load_dotenv(dotenv_path=Path(__file__).parent / '.env')

# Db connection function
def db_conn():
    # Connect to your database
    conn = psycopg2.connect(
        user=os.getenv("home_USER"),
        password=os.getenv("home_PASSWORD"),
        dbname=os.getenv("home_NAME"),
        host=os.getenv("home_HOST"),
        port=os.getenv("home_PORT")
    )
    return conn

# using the db_conn() function, it fetches source credentials to login
def get_creds(source_id, conn):
    query = f"""SELECT auth ->> 'user' as username,
                    auth ->> 'loginUrl' as loginurl,
                    auth ->> 'password' as password
              FROM source
              WHERE id = {source_id}"""
    
    cursor = conn.cursor()
    cursor.execute(query)
    creds = cursor.fetchone()

    col_names = [desc[0] for desc in cursor.description]
    creds_dict = dict(zip(col_names, creds))

    return creds_dict


def data_download(creds_dict):

    username = creds_dict.get('username')
    password = creds_dict.get('password')
    loginurl = creds_dict.get('loginurl')


    with Session(login_url=loginurl, username=username, password=password) as client:
        system_metadata = client.get_system_metadata()
        
        search_result = client.search(
            resource='Property',
            resource_class='A',
            dmql_query='(LIST_87=2025-02-01T00:00:00+)',
            limit=100
        )

        search_result_list = list(search_result)
        

        if not search_result_list:
            print("kch ni rkha in kamo me")
        else:
            headers = search_result_list[0].keys()
            # print(headers)

            with open('property_data.csv', mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(headers)

                for result in search_result_list:
                    writer.writerow(result.values())

            print("Data has been successfully written to 'property_data.csv'.")

conn = db_conn()
creds = get_creds(624, conn=conn)
print(data_download(creds))
