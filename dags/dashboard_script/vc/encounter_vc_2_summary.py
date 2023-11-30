import pandas as pd
import psycopg2
import pymongo
import urllib
from dashboard_script.connection import * 
from dashboard_script.function import *
from dashboard_script.constants import TOKEN_BOT_CHANNEL
from dashboard_script.helper import telegram_msg

def get_encounter_vc_data():
    # connection_string = 'mongodb://Nexus:'+ urllib.parse.quote("P@ssw0rd") +'@ac-wndusca-shard-00-00.h34npgh.mongodb.net:27017,ac-wndusca-shard-00-01.h34npgh.mongodb.net:27017,ac-wndusca-shard-00-02.h34npgh.mongodb.net:27017/?ssl=true&replicaSet=atlas-6kuxme-shard-0&authSource=admin&retryWrites=true&w=majority'
    # connection_string = 'mongodb://mg_master:8wY0^K&ekxG4c4foV659C9tf@10.29.209.70:8443/?authMechanism=DEFAULT'
    # client = pymongo.MongoClient(connection_string, 27017)

    connection =  get_postgreql_connection()
    client = get_mongodb_connection()

    dbname = client['TestProdDb']
    # dbname = client['uatDb']
    organization_collection = dbname['Organization']
    practitioner_collection = dbname['Practitioner']
    enconter_collection = dbname['Encounter']

    conn = None

    encounter_data = list(enconter_collection.find({'class.code':'VR','meta.source':{'$regex':'^http://person'}},{'period':1,'meta':1,'status': 1,'serviceProvider':1}).sort('period.start',1))

    encounter_df = pd.json_normalize(encounter_data)

    encounter_df['serviceProvider.reference'] = encounter_df['serviceProvider.reference'].str.replace('Organization/', '')


    organization_data = list(organization_collection.find({},{
            '_id':0,
            'id':1,
            'name':1,
            'address':1
            }))


    excludeId = ['MHNEXUS', 'MHN3']
    organization_df = pd.json_normalize(organization_data)
    organization_df = organization_df[~organization_df['id'].isin(excludeId)]
    organization_df

    left = encounter_df
    right = organization_df

    merge_df = left.merge(right, left_on='serviceProvider.reference', right_on='id' )

    def source_function(source_list):
        for entry in source_list:
            if pd.notna(source_list):
                if 'cprc' in source_list:
                    return 'CPRC'
                elif 'provider' in source_list:
                    return 'PROVIDER'
                elif 'bbis' in source_list:
                    return 'BBIS'
                elif 'phis' in source_list:
                    return 'BBIS'
                else:
                    return 'BBIS'
            return ''
        return ''

    merge_df['meta.source'] = merge_df['meta.source'].apply(source_function)

    # Convert 'encounter_date' to datetime format with specified input format and utc=True
    merge_df['period.start'] = pd.to_datetime(merge_df['period.start'], format='%Y-%m-%dT%H:%M:%S%z', utc=True)

    # Change the format to 'YYYY-MM-DD'
    merge_df['period.start'] = merge_df['period.start'].dt.strftime('%Y-%m-%d')

    encounter_new_df = pd.DataFrame({
        'organization_id': merge_df['id'],
        'encounter_date': merge_df['period.start'],
        'organization_name': merge_df['name'],
        'state_id': merge_df['address'].apply(lambda x: x[0]['extension'][0]['valueCodeableConcept']['coding'][0]['code'] if x else ''),
        'state_name': merge_df['address'].apply(lambda x: x[0]['extension'][0]['valueCodeableConcept']['coding'][0]['display'] if x else ''),
        'status': merge_df['status'],
        'source': merge_df['meta.source']
    })

    encounter_count_df = encounter_new_df.value_counts().reset_index(name='total_encounter')

    encounter_count_df = encounter_count_df[['organization_id','encounter_date','organization_name','total_encounter','state_id','state_name','source','status']]

    # conn = psycopg2.connect(
    #     database="dashboard",
    #     host="10.29.209.55",
    #     options="-c search_path=legacy,golf",
    #     user="mhnusr",
    #     password="mHnY0uS3r456&890",
    #     port="5444"
    #     )

    # conn = psycopg2.connect(
    #     database="staging",
    #     host="103.91.65.22",
    #     options="-c search_path=legacy,golf",
    #     user="mhnusr",
    #     password="mHnY0uS3r456&890",
    #     port="5093"
    #     )

    table_name = "encounter_vc_summary"
    conn = connection
    cursor = conn.cursor()

    sql = "truncate table encounter_vc_summary restart identity;"

    try:
        # execute the INSERT statement
        cursor.execute(sql)
        # commit the changes to the database
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    try:
        for _, row in encounter_count_df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (organization_id,encounter_date,organization_name,total_encounter,state_id,state_name,source,status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            values = (
                row['organization_id'],
                row['encounter_date'],
                row['organization_name'],
                row['total_encounter'],
                row['state_id'],
                row['state_name'],
                row['source'],
                row['status']
            )
            cursor.execute(insert_query, values)
            conn.commit()

        print("Data inserted successfully!")
        # telegram_msg(msg='Done update encounter vc summary', conf=TOKEN_BOT_CHANNEL)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Step 7: Close the connection
        cursor.close()
        conn.close()    

# get_encounter_vc_data()
