import boto3
from botocore.exceptions import ClientError
import json


def get_secret():

    secret_name = "cldtstdb_password"
    region_name = "eu-north-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']

    secret_dict = json.loads(secret)   # turn string into dict
    return secret_dict

if __name__ == "__main__":
    creds = get_secret()

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

# Replace with your real values
DB_USER = 'postgres'
DB_PASSWORD = creds["password"]
DB_HOST = 'cldtstdb.c78c0w2g01os.eu-north-1.rds.amazonaws.com'
DB_PORT = '5432'
DB_NAME = 'postgres'

# Create the connection string
engine = create_engine(
    f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
)

# Optional: Create the table if it doesn't exist
# If table doesn't exist then it will be created, if it's created then it's ignored.
from sqlalchemy import text
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS customers (
            full_name TEXT NOT NULL,
            state TEXT NOT NULL
        )
    """))
    conn.commit()  # commit the transaction

# adding new table 02/09/25 to play with joins
from sqlalchemy import text
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cust_details (
            customer_id TEXT NOT NULL,
            email TEXT NOT NULL
        )
    """))
    conn.commit()  # commit the transaction

dfcust = pd.DataFrame({
    'customer_id': ['101','102','103'],
    'email': ['alicesmith@example.com', 'bobjoh@data.com', 'carlosrey@jims.com']
})

dfcust.to_sql('cust_details', engine, if_exists='replace',index=False)

#adding email to original table
with engine.begin() as connemail:
  connemail.execute(text("""
        ALTER TABLE customers
        ADD COLUMN IF NOT EXISTS email TEXT
    """))

# Simulate loading from a CSV
df = pd.DataFrame({
    'full_name': ['Alice Smith', 'Bob Johnson', 'Carlos Reyes'],
    'state': ['CA', 'NY', 'TX'],
    'email': ['alicesmith@example.com', 'bobjoh@data.com', 'carlosrey@jims.com']
})

# Load the data into the database
df.to_sql('customers', engine, if_exists='replace', index=False)

print("✅ Customer data loaded!")

with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM customers"))
    for row in result:
        print(row)

with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM cust_details"))
    for row in result:
        print(row)

print(DB_USER, DB_NAME, DB_HOST, DB_PORT)