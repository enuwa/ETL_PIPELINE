import requests
import pandas as pd
import csv
import json
import psycopg2


# API  request, Web scraping to get raw data
url = "https://api.rentcast.io/v1/properties/random?limit=1000"

headers = {
    "accept": "application/json",
    "X-Api-Key": "c5b26e4991c34be2964a7f75d024518e"
}
  #Request data
response = requests.get(url, headers=headers)

# print(response.json())
data = response.json()

# Save the data to a file 
#filename = 'Rawdata/RandProptyRecords.json'
filename = 'RandProptyRecords.json'
with open(filename, 'w') as file:
    json.dump(data, file, indent=4)
    
    import json

with open('RandomPropertyRecords.json') as f:
    data = json.load(f)

print(type(data))  # list or dict?
print(data[:2])    # peek into the first few records


# Read into a dataframe
df = pd.read_json('RandomPropertyRecords.json')

# Tansformation Layer

# Convert the nested json data parameters features into strings or flatten it out.

# 1st step convert dictionary column to string:

df['features'] = df['features'].apply(json.dumps)
df['taxAssessments'] = df['taxAssessments'].apply(json.dumps)
df['propertyTaxes'] = df['propertyTaxes'].apply(json.dumps)
df['subdivision'] = df['subdivision'].apply(json.dumps)
df['assessorID'] = df['assessorID'].apply(json.dumps)
df['legalDescription'] = df['legalDescription'].apply(json.dumps)
df['hoa'] = df['hoa'].apply(json.dumps)
df['owner'] = df['owner'].apply(json.dumps)
df['zoning'] = df['zoning'].apply(json.dumps)
df['formattedAddress']= df['formattedAddress'].apply(json.dumps)
df['zipCode'] = df['zipCode'].apply(json.dumps) 
print("All Data dictionaries are flattened out")



#2nd step replace NaN values with appropriate defaults or remove row/colums as necessary
df.fillna({
    'addressLine2' : 'Unknown',
    'propertyType' : 'Unknown',
    'assessorID' : 'Unknown',
    'bedrooms' : 0,
    'bathrooms': 0,
    'squareFootage' : 0,
    'lotSize': 0,
    'legalDescription' : 'Not available',
    'subdivision' : 'Not available',
    'lastSaleDate' : 'Null',
    'features': 'None',
    'taxAssessments' : 'Not available',
    'propertyTaxes' : 'Not available',
    'owner' : 'Unknown',
    'ownerOccupied': 0, 
    'lastSalePrice' : 0,
    'yearBuilt' : 0,
    'history' : 'Unknown',
    'zoning' : 'Unknown',
    'hoa' : 'Unknown'
},inplace = True)

# CREATING DIMENSION TABLES
# 1. LOCATION DIMENSION TABLE
# FEATURES OF THE PROPERTY DIMENSION TABLE
# 2. PROPERTY DIMENSION TABLE
# 3. OWNER DIMENSION TABLE

# Create location dimension table and merge with index key

location_dim = df[['addressLine1','city','state','zipCode','county','longitude','latitude']].drop_duplicates().reset_index(drop = True)
location_dim['location_id'] = location_dim.index +1


df = df.merge(location_dim[['location_id','addressLine1','city','state','zipCode','county','longitude','latitude']],
        on= ['addressLine1','city','state','zipCode','county','longitude','latitude' ],
        how='left'
             )
location_dim.head()

# Create features dimension table and merge with the index key
features_dim = df[['features','propertyType','zoning']].drop_duplicates().reset_index(drop = True)
features_dim['features_id'] = features_dim.index +1


df = df.merge(features_dim[['features_id','features','propertyType','zoning',]],
        on= ['features','propertyType','zoning'],
        how='left'
             )
features_dim.head()

# Create the owner dimension table and merge with the index key

owner_dim = df[['owner','ownerOccupied']].drop_duplicates().reset_index(drop = True)

owner_dim['owner_id'] = owner_dim.index +1

df = df.merge(owner_dim[['owner','ownerOccupied','owner_id']],
        on= ['owner','ownerOccupied'],
        how='left'
             )
owner_dim.head()


#create date dimension table and merge with index key
import datetime as dt
from datetime import datetime, timedelta

df['lastSaleDate'] = pd.to_datetime(df['lastSaleDate'], errors="coerce", utc=True)   
df['year'] = df['lastSaleDate'].dt.year
df['month'] = df['lastSaleDate'].dt.month_name()   
df['quarter'] = df['lastSaleDate'].dt.quarter
df['day_name'] = df['lastSaleDate'].dt.day_name()


#create date dimension table and merge with index key

date_dim = df[['day_name','month','quarter','year','lastSaleDate']].drop_duplicates().reset_index(drop =True)
date_dim['date_id'] = date_dim.index +1

df = df.merge(date_dim[['day_name','month','quarter','year','lastSaleDate','date_id']],
        on= ['day_name','month','quarter','year','lastSaleDate'],
        how='left'
             )
date_dim.head()

#create property dimension table and merge with index key

property_dim = df[['propertyType','assessorID','bedrooms', 'bathrooms','squareFootage','yearBuilt','legalDescription','subdivision','zoning']].drop_duplicates().reset_index(drop = True)
property_dim['property_id'] = property_dim.index +1

df = df.merge(property_dim[['property_id','assessorID','propertyType','bedrooms','bathrooms','yearBuilt','legalDescription','subdivision','squareFootage','zoning']],
                                                  
        on= ['assessorID','propertyType','bedrooms','bathrooms','yearBuilt','legalDescription','subdivision','squareFootage','zoning'],
        how='left'
             )
property_dim.head()


#create transaction fact columns and table
fact_columns =['property_id','date_id','owner_id','features_id','location_id','lastSalePrice','propertyTaxes','taxAssessments']   
fact_table = df[fact_columns]


# Create  CSV  files for the dimension tables and fact table.

#converting files into CSV
fact_table.to_csv(r'C:\Users\admin\Desktop\ETL_PIPELINE\data\fact_table.csv', index=False)
features_dim.to_csv(r'C:\Users\admin\Desktop\ETL_PIPELINE\data\features_dimension.csv', index=False)
location_dim.to_csv(r'C:\Users\admin\Desktop\ETL_PIPELINE\data\location_dimension.csv', index = False)
owner_dim.to_csv(r'C:\Users\admin\Desktop\ETL_PIPELINE\data\owner_dimension.csv', index = False)
date_dim.to_csv(r'C:\Users\admin\Desktop\ETL_PIPELINE\data\date_dimension.csv', index=False)
property_dim.to_csv(r'C:\Users\admin\Desktop\ETL_PIPELINE\data\property_dimension.csv', index = False)

# CREATE THE POSTGRES PIPELINE
#  CREATE SCHEMA AND TABLES IN POSTGRESQL
# Connect to PostgreSQL database

#Loading Layer
#develop a function to connect to pgadmin
def get_db_connection():
    connection = psycopg2.connect(
        host = 'localhost',
        database = 'zipco_db',
        user = 'postgres',
        password = 'admin'
    )
    return connection
conn = get_db_connection()

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create schema
    cursor.execute("CREATE SCHEMA IF NOT EXISTS zipco;")

    # Drop tables if they exist
    cursor.execute("DROP TABLE IF EXISTS zipco.transaction_fact_table;")
    cursor.execute("DROP TABLE IF EXISTS zipco.location_dim;")
    cursor.execute("DROP TABLE IF EXISTS zipco.features_dim;")
    cursor.execute("DROP TABLE IF EXISTS zipco.owner_dim;")
    cursor.execute("DROP TABLE IF EXISTS zipco.date_dim;")
    cursor.execute("DROP TABLE IF EXISTS zipco.property_dim;")

    # Create dimension tables
    cursor.execute("""
        CREATE TABLE zipco.location_dim (
            addressLine1 VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(50),
            zipCode INTEGER,
            county VARCHAR(100),
            longitude NUMERIC,
            latitude NUMERIC,
            location_id INTEGER PRIMARY KEY
        );
    """)

    cursor.execute("""
        CREATE TABLE zipco.features_dim (
            features TEXT,
            propertyType TEXT,
            zoning TEXT,
            features_id INTEGER PRIMARY KEY
        );
    """)

    cursor.execute("""
        CREATE TABLE zipco.owner_dim (
            owner TEXT,
            ownerOccupied NUMERIC,
            owner_id INTEGER PRIMARY KEY
        );
    """)

    cursor.execute("""
        CREATE TABLE zipco.date_dim (
            day_name VARCHAR(10),
            month VARCHAR(10),
            quarter FLOAT,
            year FLOAT,
            lastSaleDate DATE,
            date_id INTEGER PRIMARY KEY
        );
    """)

    cursor.execute("""
        CREATE TABLE zipco.property_dim (
            propertyType TEXT,
            assessorID TEXT,
            bedrooms NUMERIC,
            bathrooms NUMERIC,
            squareFootage NUMERIC,
            yearBuilt NUMERIC,
            legalDescription TEXT,
            subdivision TEXT,
            zoning TEXT,
            property_id INTEGER PRIMARY KEY
        );
    """)

    # Create fact table with foreign keys
    cursor.execute("""
        CREATE TABLE zipco.transaction_fact_table (
            transaction_id SERIAL PRIMARY KEY,
            property_id INTEGER REFERENCES zipco.property_dim(property_id),
            date_id INTEGER REFERENCES zipco.date_dim(date_id),
            owner_id INTEGER REFERENCES zipco.owner_dim(owner_id),
            features_id INTEGER REFERENCES zipco.features_dim(features_id),
            location_id INTEGER REFERENCES zipco.location_dim(location_id),
            lastSaleDate DATE,
            lastSalePrice NUMERIC,
            propertyTaxes NUMERIC,
            taxAssessments TEXT   
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()
create_tables()

# Load All Tales Into PostgreSQL Database
def load_data_from_csv_to_table(csv_path, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # If table_name contains a dot, split schema and table for safe quoting
    if '.' in table_name:
        schema, table = table_name.split('.')
        table_name = f'"{schema}"."{table}"'
    else:
        table_name = f'"{table_name}"'
    
    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header

        for row in reader:
            row = [None if value == '' else value for value in row]
    
            placeholders = ','.join(['%s'] * len(row))
            query = f'INSERT INTO {table_name} VALUES ({placeholders})'
            cursor.execute(query, row)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    #location dimension loading values

location_csv_path = r"C:\Users\admin\Desktop\ETL_PIPELINE\data\location_dimension.csv"
load_data_from_csv_to_table(location_csv_path,'zipco.location_dim')

features_csv_path = r"C:\Users\admin\Desktop\ETL_PIPELINE\data\features_dimension.csv"
load_data_from_csv_to_table(features_csv_path, 'zipco.features_dim')

# Owner dimension loading values

owner_csv_path = r"C:\Users\admin\Desktop\ETL_PIPELINE\data\owner_dimension.csv"
load_data_from_csv_to_table(owner_csv_path,'zipco.owner_dim')

import pandas as pd
transaction_fact_table =pd.read_csv(r'C:\Users\admin\Desktop\ETL_PIPELINE\data\fact_table.csv')

#Date dimension loding values 

date_csv_path = r"C:\Users\admin\Desktop\ETL_PIPELINE\data\date_dimension.csv"
load_data_from_csv_to_table(date_csv_path,'zipco.date_dim')

#properties dimension loding values 

property_csv_path = r"C:\Users\admin\Desktop\ETL_PIPELINE\data\property_dimension.csv"
load_data_from_csv_to_table(property_csv_path, 'zipco.property_dim')

import pandas as pd
from sqlalchemy import create_engine

def load_data_from_csv_to_table(csv_path, table_name, db_url="postgresql://postgres:admin@localhost/zipco_db"):
    # Load CSV into DataFrame
    df = pd.read_csv(csv_path)

    # Connect to the database
    engine = create_engine(db_url)

    # Load data into the specified table
    df.to_sql(name=table_name.split('.')[-1],  # only table name, not schema
              con=engine,
              schema=table_name.split('.')[0],  # schema part
              if_exists='replace',  # or 'append'
              index=False)
    print(f"Data loaded successfully into {table_name}")

fact_table_csv_path = r"C:\Users\admin\Desktop\ETL_PIPELINE\data\fact_table.csv"
load_data_from_csv_to_table(fact_table_csv_path,'zipco.transaction_fact_table')



print("All Data loaded successfully into the their  respective Schema and tables")


