import random
import string
import pandas as pd
from constants import *
import psycopg2

from dotenv import load_dotenv
import os
load_dotenv()

import io
import csv


def get_unique_id():
    while True:
        number = str(''.join(random.choices(string.digits, k=8)))
        if not number.startswith('0'):
            return number

def get_new_ids(df):
    # original_ids = df[type+'_unique_identifier'].unique()
    new_ids= []
    for i in range(0, len(df)):
        new_ids.append(get_unique_id())
    return new_ids

def isNaN(string):
    return string == string

def read_file(file, sheet_name):
    df = pd.read_excel(file, sheet_name= sheet_name)
    return df
        
def id_mapper(df, table_name, name, id):
    mapper = {row[name]: row[id] for index, row in df.iterrows()}
    db_df = query_from_db(table_name)
    print(mapper)
    for index, row in db_df.iterrows():
        mapper[row[name]] = str(row[id])
    print(mapper)
    return mapper

def rename_columns(df):
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
    return df

def add_who_columns(df):
    df['created_by'] = user
    df['created_date'] = current_date
    df['updated_by'] = user
    df['updated_date'] = current_date

    return df

def jdbc_connection():

    host= os.environ['host']
    user= os.environ['user']
    port= os.environ['port']
    password= os.environ['password']
    database= os.environ['database']

    conn = psycopg2.connect(host= host, user= user, password= password, database= database)
    conn.autocommit= True

    return conn

def insert_into_db(table_name, db_columns, df, df_columns):
    conn = jdbc_connection()

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    for index, row in df.iterrows():
        record = [row[i] for i in df_columns]
        writer.writerow(record)

    csv_buffer.seek(0)
    print(csv_buffer.getvalue())

    with conn.cursor() as cur:
        postgres_insert_query = f"COPY {table_name} ({db_columns}) FROM STDIN WITH(FORMAT CSV, DELIMITER ',',NULL '')"
        cur.copy_expert(postgres_insert_query, csv_buffer)

def query_from_db(table_name):
    conn = jdbc_connection()
    df = pd.read_sql(f"select * from {table_name}", conn)
    print(df)
    return df