
# ETL for cryptocurrency price tracking. 

#LIBRARIES 
import requests
import time 
import os
from coinbase.wallet.client import Client # for interacting with coinbase API
from dotenv import load_dotenv #for using environmental variables 
import psycopg2 # for creating python-postgres connection 
from datetime import datetime 

def main(): 
    # Loading environment variables
    load_dotenv("./.env")

    #extracting API and database credentials from .env
    api_key = os.getenv("api_key")
    api_secret = os.getenv("api_secret")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    #creating connection to railway postgres db: 
    connection = psycopg2.connect(
        host = db_host, 
        database = db_name, 
        user = db_user, 
        password = db_password, 
        port = db_port
    )

    #initializing cursor
    cursor = connection.cursor() 

    # EXTRACT + TRANSFORM 

    #establishing connection to coinbase API with credentials 
    client = Client(api_key, api_secret)
    coins = ['BTC-USD', 'ETH-USD', 'NEAR-USD']

    #creating function to pull price data from coinbase API using client 
    def spot_price(client, c_pair): 
        # Fetch the spot price for a given currency pair
        return client.get_spot_price(currency_pair=c_pair)
        
    try:

        #creating dictionary of 3 coins by calling spot_price function
        prices = {i: spot_price(client, i) for i in coins}

        #throw an error if any coin data is not available
        if any(not price for price in prices.values()):
            raise ValueError("Incomplete data received from API")

    # LOAD 

        #creating schema for organize tables: 
        cursor.execute("CREATE SCHEMA IF NOT EXISTS crypto;")
        cursor.execute("GRANT ALL PRIVILEGES ON SCHEMA crypto to postgres;")

        #making sure timestamp will display in my local time
        cursor.execute("SET TIMEZONE = 'America/Los_Angeles';") 

        connection.commit() 

                # creating 3 tables for each coin in postgres db
        table_schema_btc = ("""
                            CREATE TABLE IF NOT EXISTS crypto.bitcoin
                            (id serial PRIMARY KEY, 
                            datetime timestamp, 
                            base varchar(10), 
                            amount numeric(10,4),
                            currency char(3));
                            """)
                
        table_schema_eth = ("""
                            CREATE TABLE IF NOT EXISTS crypto.ethereum
                            (id serial PRIMARY KEY, 
                            datetime timestamp, 
                            base varchar(10), 
                            amount numeric(10,4),
                            currency char(3));
                            """)
                
        table_schema_near = ("""
                            CREATE TABLE IF NOT EXISTS crypto.near
                            (id serial PRIMARY KEY, 
                            datetime timestamp, 
                            base varchar(10), 
                            amount numeric(10,4),
                            currency char(3));
                            """)
                
        tables = [table_schema_btc, table_schema_eth, table_schema_near]

        #iterating through create table statements 
        for t in tables: 
            cursor.execute(t)
            
        connection.commit()

            #inserting data into tables: 
        insert_query_btc = """ INSERT INTO crypto.bitcoin (datetime, base, amount, currency)
                                VALUES (CURRENT_TIMESTAMP, %(base)s, %(amount)s, %(currency)s);
                                """
        insert_query_eth = """ INSERT INTO crypto.ethereum (datetime, base, amount, currency)
                                VALUES (CURRENT_TIMESTAMP, %(base)s, %(amount)s, %(currency)s);
                                """
        insert_query_near = """ INSERT INTO crypto.near (datetime, base, amount, currency)
                                VALUES (CURRENT_TIMESTAMP, %(base)s, %(amount)s, %(currency)s);
                                """
        

        cursor.execute(insert_query_btc, prices["BTC-USD"])
        cursor.execute(insert_query_eth, prices["ETH-USD"])
        cursor.execute(insert_query_near, prices["NEAR-USD"])

        connection.commit()

    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except psycopg2.DatabaseError as db_err:
        print(f"Database error occurred: {db_err}")
        connection.rollback()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        connection.rollback()

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("ETL Complete")

while True: 
    main() 
    print("sleeping for 4 hours")
    time.sleep(14400)




