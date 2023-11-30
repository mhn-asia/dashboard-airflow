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

def get_vaccine_defaulted_update(**kwargs):

    connection =  get_postgreql_connection()
    client = get_mongodb_connection()

    dbname = client['TestProdDb']
    immunizationRecommendation_collection = dbname['ImmunizationRecommendation']
    organization_collection = dbname['Organization']

    execution_date = kwargs['execution_date']
    yesterday = execution_date - timedelta(days=1)
    yesterday_string = yesterday.strftime("%Y-%m-%d")
    # yesterday_string = '2023-07'

    conn = None

    pd.set_option('display.max_colwidth', None)

    immuneRcmdData = list(immunizationRecommendation_collection.find({'recommendation.forecastStatus.coding.code': 'defaulted','meta.lastUpdated': {'$regex': yesterday_string},'authority':{'$exists': 1}},{'_id':0,'id':1,'meta.lastUpdated':1,'recommendation':1, 'authority': 1}))

    if not immuneRcmdData:  # Check if there is no data
        print("No data to process.")
        telegram_msg(msg='No vaccine_defaulted data for date '+yesterday_string, conf=TOKEN_BOT_CHANNEL)
        # Handle this case as needed, such as inserting a default row or taking appropriate action.
    else:
        immuneRcmd_df = pd.json_normalize(immuneRcmdData)
        immuneRcmd_df['meta.lastUpdated'] = pd.to_datetime(immuneRcmd_df['meta.lastUpdated'], format='%Y-%m-%dT%H:%M:%S%z', utc=True)
        immuneRcmd_df['meta.lastUpdated'] = immuneRcmd_df['meta.lastUpdated'].dt.strftime('%Y-%m-%d')


        immuneRcmd_df['recommendation.extension'] = immuneRcmd_df['recommendation'].apply(
            lambda x: x[0]['extension'][0]['valueBoolean'] if (isinstance(x, list) and len(x) > 0 and isinstance(x[0], dict) and 'extension' in x[0]) else None
        )

        def determine_value(row):
            if isinstance(row, list):
                for item in row:
                    if 'extension' in item and isinstance(item['extension'], list):
                        for ext in item['extension']:
                            if ext.get('url') == "http://fhir.hie.moh.gov.my/StructureDefinition/extension-booster-my-core" and ext.get('valueBoolean', False):
                                return 4
                    if 'doseNumberPositiveInt' in item:
                        return item['doseNumberPositiveInt']
            return None

        immuneRcmd_df['dose'] = immuneRcmd_df['recommendation'].apply(determine_value)
        pd.set_option('display.max_colwidth', None)
        immuneRcmd_df['dose'] = immuneRcmd_df['dose'].astype(pd.Int64Dtype()) 

        immuneRcmd_df['authority.reference'] = immuneRcmd_df['authority.reference'].str.replace('Organization/','')

        organization_data = list(organization_collection.find({},{
                '_id':0,
                'id':1,
                'name':1,
                'address':1
                }))


        excludeId = ['HIE', 'PH', 'MPIS', 'BBIS']
        organization_df = pd.json_normalize(organization_data)
        organization_df = organization_df[~organization_df['id'].isin(excludeId)]

        left = immuneRcmd_df
        right = organization_df


        merge_df = left.merge(right, left_on='authority.reference', right_on='id' )

        immuneRcmd_new_df = pd.DataFrame({
            'organization_id': merge_df['authority.reference'],
            'organization_name': merge_df['name'],
            'date' : merge_df['meta.lastUpdated'],
            'code': merge_df['recommendation'].apply(lambda x: x[0]['vaccineCode'][0]['coding'][0]['code'] if x else ''),
            'display': merge_df['recommendation'].apply(lambda x: x[0]['vaccineCode'][0]['coding'][0]['display'] if x else ''),
            'state_id': merge_df['address'].apply(lambda x: x[0]['extension'][0]['valueCodeableConcept']['coding'][0]['code'] if x and x[0]['extension'] and 'valueCodeableConcept' in x[0]['extension'][0] and 'coding' in x[0]['extension'][0]['valueCodeableConcept'] and x[0]['extension'][0]['valueCodeableConcept']['coding'] else ''),
            'state_name': merge_df['address'].apply(lambda x: x[0]['extension'][0]['valueCodeableConcept']['coding'][0]['display'] if x and x[0]['extension'] and 'valueCodeableConcept' in x[0]['extension'][0] and 'coding' in x[0]['extension'][0]['valueCodeableConcept'] and x[0]['extension'][0]['valueCodeableConcept']['coding'] else ''),
            'dose' : merge_df['dose']
        })

        immuneRcmd_new_df_counts = immuneRcmd_new_df.value_counts().reset_index(name='Count')


        table_name = "vaccine_defaulted"
        conn = connection
        cursor = conn.cursor()

        try:
            for _, row in immuneRcmd_new_df_counts.iterrows():
                insert_query = f"""
                INSERT INTO {table_name} (organization_id, organization_name, date, code, display, state_id, state_name, dose , count)
                VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s);
                """
                values = (
                    row['organization_id'],
                    row['organization_name'],
                    row['date'],
                    row['code'],
                    row['display'],
                    row['state_id'],
                    row['state_name'],
                    row['dose'],
                    row['Count']
                )
                cursor.execute(insert_query, values)
                conn.commit()

            print("Data inserted successfully!")
            telegram_msg(msg='Done update vaccine_defaulted for date '+yesterday_string, conf=TOKEN_BOT_CHANNEL)

        except Exception as e:
            print(f"Error: {e}")

        finally:
            # Step 7: Close the connection
            cursor.close()
            conn.close()    
    
# get_vaccine_defaulted_update()