import pandas as pd
import psycopg2
from psycopg2 import extras
import os
from dotenv import load_dotenv
from pathlib import Path
import requests
from requests.auth import HTTPDigestAuth
import xml.etree.ElementTree as ET

load_dotenv(dotenv_path=Path(__file__).parent / '.env')

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



def login(creds_dict):
    headers = {
        "RETS-Version": "RETS/1.5",
        "User-Agent": "PostmanRuntime/7.29.2",
        "Accept": "*/*",
        "Connection": "keep-alive"
    }
    
    username = creds_dict.get('username')
    password = creds_dict.get('password')
    loginurl = creds_dict.get('loginurl')

    session = requests.Session()
    session.auth = HTTPDigestAuth(username, password)

    response = session.get(loginurl, headers=headers)

    if response.status_code == 200:
        return session
    else:
        return None

def get_count(session):

    rets_search_url = "http://retsgw.flexmls.com/rets2_3/Search"

    headers = {
        "RETS-Version": "RETS/1.5",
        "User-Agent": "PostmanRuntime/7.29.2",
        "Accept": "*/*",
        "Connection": "keep-alive"
    }

    params = {
        "SearchType": "Property",
        "Class": "A",
        "QueryType": "DMQL2",
        "Query": "(LIST_87=2024-12-01T00:00:00+)",
        "Format": "STANDARD-XML",
        "Count": "2",
        # "Limit": "1"
    }

    response = session.get(rets_search_url, headers=headers, params=params)
    if response.status_code == 200:
        root = ET.fromstring(response.text)
        count_element = root.find('.//COUNT')
        count_value = int(count_element.get('Records'))

        return count_value
    else:
        print("Ni bni baat")


conn = db_conn()
creds = get_creds(591, conn)
session = login(creds_dict=creds)
print(get_count(session=session))