# sql_db.py
#This Python script is used to interact with an SQLite database. 
#It includes functions to 
# create a connection to the database, 
# create a table, 
# insert data into a table, 
# query the database, and 
# get a JSON-like representation of the database schema.

# The script uses the sqlite3 module to interact with the SQLite database. 
# The pandas module is used to convert the result of SQL queries into dataframes for easier manipulation.

import sqlite3
from sqlite3 import Error
import random
from datetime import date, timedelta
from tqdm import tqdm
import pandas as pd

DATABASE_NAME = "joblist.db"

#The create_connection function establishes a connection to the SQLite database. If the database does not exist, it is created.
def create_connection():
    """ Create or connect to an SQLite database """
    conn = None;
    try:
        conn = sqlite3.connect(DATABASE_NAME)
    except Error as e:
        print(e)
    return conn

#The create_table function creates a new table in the database using a provided SQL command.
def create_table(conn, create_table_sql):
    """ Create a table with the specified SQL command """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

# The insert_data function inserts a new row of data into a specified table in the database.
def insert_data(conn, table_name, data_dict):
    """ Insert a new data into a table """
    columns = ', '.join(data_dict.keys())
    placeholders = ', '.join('?' * len(data_dict))
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    cur = conn.cursor()
    cur.execute(sql, list(data_dict.values()))
    conn.commit()
    return cur.lastrowid

# The query_database function runs a provided SQL query and returns the results in a pandas dataframe.
def query_database(query):
    """ Run SQL query and return results in a dataframe """
    conn = create_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# The setup_joblist_table function creates a table named "joblist" and inserts 13 rows of data into it.
def setup_joblist_table():
    conn = create_connection()
    sql_create_joblists_table = """
    CREATE TABLE IF NOT EXISTS joblist (
        job_id INTEGER PRIMARY KEY,
        due_date TEXT NOT NULL,
        job_title TEXT NOT NULL,
        vessel_name TEXT NOT NULL,
        component TEXT NOT NULL,
        maker TEXT NOT NULL,
        model TEXT NOT NULL
    );
    """
    create_table(conn, sql_create_joblists_table)

    # Insert 13 rows of data
    data_list = [
        {
            "job_id": 1,
            "due_date": "13/2/30",
            "job_title": "RNW0023A - Purifiers Bearing and Motor Overhaul",
            "vessel_name": "Hafnia Andrea",
            "component": "HFO purifier 1",
            "maker": "[MKR]ALFA LAVAL",
            "model": "S 937"
        },
        {
            "job_id": 2,
            "due_date": "30/8/36",
            "job_title": "OVH0022A - Purifier Major Overhaul Alfa Laval",
            "vessel_name": "Hafnia Andrea",
            "component": "ME LO purifier 1",
            "maker": "[MKR]ALFA LAVAL",
            "model": "S 815"
        },
        {
            "job_id": 3,
            "due_date": "21/6/69",
            "job_title": "RNW0023A - Purifiers Bearing and Motor Overhaul",
            "vessel_name": "Hafnia Andrea",
            "component": "ME LO purifier 1",
            "maker": "[MKR]ALFA LAVAL",
            "model": "S 815"
        },
        {
            "job_id": 4,
            "due_date": "19/2/24",
            "job_title": "Purifier Service and Bowl Cleaning",
            "vessel_name": "Hafnia Andrea",
            "component": "ME LO purifier 2",
            "maker": "[MKR]ALFA LAVAL",
            "model": "S 815"
        },
        {
            "job_id": 5,
            "due_date": "22/8/24",
            "job_title": "Purifier Service and Bowl Cleaning",
            "vessel_name": "Hafnia Andrea",
            "component": "ME LO purifier 2",
            "maker": "[MKR]ALFA LAVAL",
            "model": "S 815"
        },
        {
            "job_id": 6,
            "due_date": "23/8/24",
            "job_title": "Purifier Major Overhaul Alfa Laval",
            "vessel_name": "Hafnia Andrea",
            "component": "ME LO purifier 2",
            "maker": "[MKR]ALFA LAVAL",
            "model": "S 815"
        },
        {
            "job_id": 7,
            "due_date": "21/9/24",
            "job_title": "RNW0023A - Purifiers Bearing and Motor Overhaul",
            "vessel_name": "Hafnia Andrea",
            "component": "HFO purifier 2",
            "maker": "[MKR]ALFA LAVAL",
            "model": "S 937"
        },
        {
            "job_id": 8,
            "due_date": "25/11/24",
            "job_title": "Purifier Service and Cleaning Alfa Laval",
            "vessel_name": "Hafnia Andrea",
            "component": "HFO purifier 2",
            "maker": "[MKR]ALFA LAVAL",
            "model": "S 937"
        },
        {
            "job_id": 9,
            "due_date": "9/4/25",
            "job_title": "Purifier Service and Cleaning Alfa Laval",
            "vessel_name": "Hafnia Andrea",
            "component": "HFO purifier 1",
            "maker": "[MKR]ALFA LAVAL",
            "model": "S 937"
        },
        {
            "job_id": 10,
            "due_date": "24/9/25",
            "job_title": "Purifier Major Overhaul Alfa Laval",
            "vessel_name": "Hafnia Andrea",
            "component": "HFO purifier 2",
            "maker": "[MKR]ALFA LAVAL",
            "model": "S 937"
        },
        {
            "job_id": 11,
            "due_date": "13/1/27",
            "job_title": "Purifiers Bearing and Motor Overhaul",
            "vessel_name": "Hafnia Andrea",
            "component": "ME LO purifier 2",
            "maker": "[MKR]ALFA LAVAL",
            "model": "S 815"
        },
        {
            "job_id": 12,
            "due_date": "14/10/27",
            "job_title": "Purifier Service and Bowl Cleaning",
            "vessel_name": "Hafnia Andrea",
            "component": "ME LO purifier 1",
            "maker": "[MKR]ALFA LAVAL",
            "model": "S 815"
        },
        {
            "job_id": 13,
            "due_date": "5/5/28",
            "job_title": "Purifier Major Overhaul Alfa Laval",
            "vessel_name": "Hafnia Andrea",
            "component": "HFO purifier 1",
            "maker": "[MKR]ALFA LAVAL",
            "model": "S 937"
        }
    ]
    for data in data_list:
        insert_data(conn, "joblist", data)
    conn.close()

def create_makermodels_table_from_csv(csv_file):
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)

    # Create a connection to the SQLite database
    conn = create_connection()

    # Create the makermodels table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS makermodels (
        func_no TEXT NOT NULL,
        func_desc TEXT NOT NULL,
        vessel_code TEXT NOT NULL,
        vessel_name TEXT NOT NULL,
        maker TEXT NOT NULL,
        model TEXT
    );
    """
    create_table(conn, create_table_sql)

    # Insert the data from the DataFrame into the makermodels table
    df.to_sql('makermodels', conn, if_exists='append', index=False)

    # Close the connection to the SQLite database
    conn.close()

def create_workorders_table_from_csv(csv_file):
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)

    # Create a connection to the SQLite database
    conn = create_connection()

    # Create the workorders table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS workorders (
        job_title TEXT NOT NULL,
        vessel_code TEXT NOT NULL,
        func_no TEXT NOT NULL,
        func_desc TEXT NOT NULL,
        due_date TEXT NOT NULL
    );
    """
    create_table(conn, create_table_sql)

    # Insert the data from the DataFrame into the workorders table
    df.to_sql('workorders', conn, if_exists='append', index=False)

    # Close the connection to the SQLite database
    conn.close()

# The get_schema_representation function returns a dictionary that represents the schema of the database. 
# The keys are table names and the values are dictionaries where the keys are column names and the values are the corresponding data types.
def get_schema_representation():
    """ Get the database schema in a JSON-like format """
    conn = create_connection()
    cursor = conn.cursor()
    
    # Query to get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    db_schema = {}
    
    for table in tables:
        table_name = table[0]
        
        # Query to get column details for each table
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        column_details = {}
        for column in columns:
            column_name = column[1]
            column_type = column[2]
            column_details[column_name] = column_type
        
        db_schema[table_name] = column_details
    
    conn.close()
    return db_schema

# This will create the table and insert 100 rows when you run sql_db.py
if __name__ == "__main__":

    # Setting up the joblists table
    # setup_joblist_table()
    #create_makermodels_table_from_csv('MakerModel.csv')
    #create_workorders_table_from_csv('WorkOrders.csv')

    # Querying the database
    #print(query_database("SELECT * FROM makermodels LIMIT 10"))
    #print(query_database("SELECT * FROM workorders"))
    
    # Getting the schema representation
    print(get_schema_representation())