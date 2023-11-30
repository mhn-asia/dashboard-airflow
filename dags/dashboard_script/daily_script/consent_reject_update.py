import pandas as pd
import psycopg2
import pymongo
import urllib
import json
from datetime import datetime,date, timedelta
from dashboard_script.connection import * 
from dashboard_script.function import *
from dashboard_script.constants import TOKEN_BOT_CHANNEL
from dashboard_script.helper import telegram_msg

def get_consent_reject_update(**kwargs):
    connection =  get_postgreql_connection()
    client = get_mongodb_connection()

    dbname = client['TestProdDb']
    consent_collection = dbname['Consent']

    execution_date = kwargs['execution_date']
    yesterday = execution_date - timedelta(days=1)
    yesterday_string = yesterday.strftime("%Y-%m-%d")
    # yesterday_string = '2023-08'

    conn = None

    pd.set_option('display.max_colwidth', None)

    consentData =  list(consent_collection.find({"status": "rejected", 'meta.lastUpdated': {'$regex': yesterday_string}},{'_id':0,'id': 1, 'status':1, 'category':1, 'meta.lastUpdated':1}))
    if not consentData:  # Check if there is no data
        print("No data to process.")
        # Handle this case as needed, such as inserting a default row or taking appropriate action.
    else:
        consent_df = pd.json_normalize(consentData)

        consent_df['meta.lastUpdated'] = pd.to_datetime(consent_df['meta.lastUpdated'], format='%Y-%m-%dT%H:%M:%S%z', utc=True)
        consent_df['meta.lastUpdated'] = consent_df['meta.lastUpdated'].dt.strftime('%Y-%m-%d')

        consent_new_df = pd.DataFrame({
            'date' : consent_df['meta.lastUpdated']
        })

        consent_new_df = consent_new_df.value_counts().reset_index(name='reject_count')

        consent_reject_summary_df = pd.DataFrame(consent_new_df)

        table_name = "consent_reject_summary"
        conn = connection
        cursor = conn.cursor()

        try:
            for _, row in consent_reject_summary_df.iterrows():
                insert_query = f"""
                INSERT INTO {table_name} (date, reject_count)
                VALUES (%s, %s);
                """
                values = (
                    row['date'],
                    row['reject_count']
                )
                cursor.execute(insert_query, values)
                conn.commit()

            print("Data inserted successfully!")
            telegram_msg(msg='Done update consent_reject for date '+yesterday_string, conf=TOKEN_BOT_CHANNEL)

        except Exception as e:
            print(f"Error: {e}")

        finally:
            # Step 7: Close the connection
            cursor.close()
            conn.close()

# get_consent_reject_update()