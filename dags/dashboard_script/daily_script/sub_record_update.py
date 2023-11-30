import pandas as pd
import psycopg2
import pymongo
import urllib
import json
from dateutil.parser import parse
from datetime import datetime,date, timedelta
from dashboard_script.connection import * 
from dashboard_script.function import *
from dashboard_script.constants import TOKEN_BOT_CHANNEL
from dashboard_script.helper import telegram_msg

def get_sub_record_update(**kwargs):
    connection =  get_postgreql_connection()
    client = get_mongodb_connection()

    dbname = client['TestProdDb']
    list_collection = dbname['List']
    organization_collection = dbname['Organization']
    practitioner_collection = dbname['PractitionerRole']


    execution_date = kwargs['execution_date']
    yesterday = execution_date - timedelta(days=1)
    yesterday_string = yesterday.strftime("%Y-%m-%d")
    # yesterday_string = '2023'

    conn = None

    pd.set_option('display.max_colwidth', None)

    list_data = list(list_collection.find({'code.coding.code':{'$regex': 'sub-00'}, 'date': {'$regex': yesterday_string}},{'_id':0,'code.coding.code': 1,'code.coding.display': 1, 'source.reference': 1, 'date': 1}))
    if not list_data:  # Check if there is no data
        print("No data to process.")
        telegram_msg(msg='No sub_record data for date '+yesterday_string, conf=TOKEN_BOT_CHANNEL)
        # Handle this case as needed, such as inserting a default row or taking appropriate action.
    else:
        list_df = pd.json_normalize(list_data)

        list_df['source.reference'] = list_df['source.reference'].str.replace('PractitionerRole/','')
        list_df['source.reference'] = list_df['source.reference'].str.replace('/_history/.*', '')

        practitioner_data = list(practitioner_collection.find({},{'_id':0,'id':1,'organization.reference': 1}))
        practitioner_df = pd.json_normalize(practitioner_data)

        practitioner_df['organization.reference'] = practitioner_df['organization.reference'].str.replace('Organization/', '')


        left = list_df
        right = practitioner_df

        merge_df = left.merge(right, left_on='source.reference', right_on='id', how='left')

        organization_data = list(organization_collection.find({},{
                '_id':0,
                'id':1,
                'name':1,
                'address':1
                }))


        excludeId = ['MHNEXUS']
        organization_df = pd.json_normalize(organization_data)
        organization_df = organization_df[~organization_df['id'].isin(excludeId)]
        organization_df

        left = merge_df
        right = organization_df

        merge2_df = left.merge(right, left_on='organization.reference', right_on='id' )

        merge2_df['date'] = pd.to_datetime(merge2_df['date'], format='%Y-%m-%dT%H:%M:%S%z', utc=True)
        merge2_df['date'] = merge2_df['date'].dt.strftime('%Y-%m-%d')

        list_new_df = pd.DataFrame({
            'organization_id' : merge2_df['id_y'],
            'organization_name' : merge2_df['name'],
            'date' : merge2_df['date'],
            'code' : merge2_df['code.coding'].apply(lambda x: x[0]['code']),
            'display' : merge2_df['code.coding'].apply(lambda x: x[0]['display']),
            'state_id': merge2_df['address'].apply(lambda x: x[0]['extension'][0]['valueCodeableConcept']['coding'][0]['code'] if x and x[0]['extension'] and 'valueCodeableConcept' in x[0]['extension'][0] and 'coding' in x[0]['extension'][0]['valueCodeableConcept'] and x[0]['extension'][0]['valueCodeableConcept']['coding'] else ''),
            'state_name': merge2_df['address'].apply(lambda x: x[0]['extension'][0]['valueCodeableConcept']['coding'][0]['display'] if x and x[0]['extension'] and 'valueCodeableConcept' in x[0]['extension'][0] and 'coding' in x[0]['extension'][0]['valueCodeableConcept'] and x[0]['extension'][0]['valueCodeableConcept']['coding'] else ''),
        })

        list_new_df_counts = list_new_df.value_counts().reset_index(name='Count')

        table_name = "sub_record_summary"
        conn = connection
        cursor = conn.cursor()

        try:
            for _, row in list_new_df_counts.iterrows():
                insert_query = f"""
                INSERT INTO {table_name} (organization_id, organization_name,date,code,display,state_id,state_name,count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                values = (
                    row['organization_id'],
                    row['organization_name'],
                    row['date'],
                    row['code'],
                    row['display'],
                    row['state_id'],
                    row['state_name'],
                    row['Count'],
                )
                cursor.execute(insert_query, values)
                conn.commit()

            print("Data inserted successfully!")
            telegram_msg(msg='Done update sub_record for date '+yesterday_string, conf=TOKEN_BOT_CHANNEL)

        except Exception as e:
            print(f"Error: {e}")

        finally:
            # Step 7: Close the connection
            cursor.close()
            conn.close() 

# get_sub_record_update()  