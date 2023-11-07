
# ETL for cryptocurrency price tracking. 

#LIBRARIES 
import requests
import time 
import os
from coinbase.wallet.client import Client # for interacting with coinbase API
from dotenv import load_dotenv #for using environmental variables 
import psycopg2 # for creating python-postgres connection 
from datetime import datetime 
from email.utils import parsedate_to_datetime
import pytz

def main(): 
    # Loading environment variables
    load_dotenv("./moon.env")

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

        #adding timestamp 
        prices["datetime"] = datetime.now()
        
        #throw an error if any coin data is not available
        if any(not price for price in prices.values()):
            raise ValueError("Incomplete data received from API")
        
    except Exception as e:
        # error log 
        print(f"An error occurred: {e}")

    # LOAD 

        # creating 3 tables for each coin in postgres db
        table_schema_btc = ("""
                    CREATE TABLE IF NOT EXISTS bitcoin
                    (id serial PRIMARY KEY, 
                     datetime timestamp, 
                     base varchar(10), 
                     amount numeric(10,4),
                     currency char(3));
                    """)
        
        table_schema_eth = ("""
                    CREATE TABLE IF NOT EXISTS ethereum
                    (id serial PRIMARY KEY, 
                     datetime timestamp, 
                     base varchar(10), 
                     amount numeric(10,4),
                     currency char(3));
                    """)
        
        table_schema_near = ("""
                    CREATE TABLE IF NOT EXISTS near
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
        insert_query = """ INSERT INTO bitcoin (datetime, base, amount, currency)
                            VALUES (%(datetime)s, %(base)s, %(amount)s, %(currency)s);
                            """


        cursor.execute(insert_query, price_data["BTC-USD"])


        cursor.execute(table_schema)

        #adding moon data dictionary to railway postgresql db
        insert_query = (
            """
            INSERT INTO moon (local_datetime, api_datetime, phase, illumination, age_days, lunar_cycle, phase_name, stage, emoji, zodiac_sign, 
            moonrise, moonrise_timestamp, moonset, moonset_timestamp, moon_altitude, moon_distance, 
            moon_azimuth, moon_parallactic_angle) 
            VALUES (%(local_datetime)s, %(date)s, %(phase)s, %(illumination)s, %(age_days)s, %(lunar_cycle)s, %(phase_name)s,
            %(stage)s, %(emoji)s, %(zodiac_sign)s, %(moonrise)s, %(moonrise_timestamp)s, %(moonset)s, 
            %(moonset_timestamp)s, %(moon_altitude)s, %(moon_distance)s, %(moon_azimuth)s, %(moon_parallactic_angle)s);
            """
        )

        cursor.execute(insert_query, moon_dict)

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
    print("sleeping for 24h)")
    time.sleep(24*60*60)




