import pandas as pd
import psycopg2
import pymongo
from datetime import datetime,date, timedelta
import urllib
from dashboard_script.connection import * 
from dashboard_script.function import *
from dashboard_script.constants import TOKEN_BOT_CHANNEL
from dashboard_script.helper import telegram_msg


def get_vaccine_administered_update(**kwargs):
    connection =  get_postgreql_connection()
    client = get_mongodb_connection()

    dbname = client['TestProdDb']
    organization_collection = dbname['Organization']
    encounter_collection = dbname['Encounter']
    immunization_collection = dbname['Immunization']

    execution_date = kwargs['execution_date']
    yesterday = execution_date - timedelta(days=1)
    yesterday_string = yesterday.strftime("%Y-%m-%d")
    # print(yesterday_string)
    # yesterday_string = '2023-07-10'

    conn = None

    immunization_data = list(immunization_collection.find({'occurrenceDateTime':{'$regex': yesterday_string}},{'_id':0,'id':1,'vaccineCode':1,'occurrenceDateTime':1,'protocolApplied':1,'encounter':1,'performer':1}))
    if not immunization_data:  # Check if there is no data
        print("No data to process.")
        telegram_msg(msg='No vaccine_administered data for date '+yesterday_string, conf=TOKEN_BOT_CHANNEL)
        # Handle this case as needed, such as inserting a default row or taking appropriate action.
    else:
        immunization_df = pd.json_normalize(immunization_collection)

        columns_to_convert = ['occurrenceDateTime']

        for col in columns_to_convert:
            # Convert 'encounter_date' to datetime format with specified input format and utc=True
            immunization_df[col] = pd.to_datetime(immunization_df[col], format='%Y-%m-%dT%H:%M:%S%z', utc=True)

            # Change the format to 'YYYY-MM-DD'
            immunization_df[col] = immunization_df[col].dt.strftime('%Y-%m-%d')

        immunization_df['vaccine_code'] = immunization_df['vaccineCode.coding'].apply(lambda x: x[0]['code'] if x else None)
        immunization_df['vaccine_name'] = immunization_df['vaccineCode.coding'].apply(lambda x: x[0]['display'] if x else None)
        immunization_df['encounter.reference'] = immunization_df['encounter.reference'].str.replace('Encounter/', '')

        immunization_df['protocolApplied.extension'] = immunization_df['protocolApplied'].apply(
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

        immunization_df['dose'] = immunization_df['protocolApplied'].apply(determine_value)
        pd.set_option('display.max_colwidth', None)
        immunization_df['dose'] = immunization_df['dose'].astype(pd.Int64Dtype()) 


        encounter_data = encounter_collection.find({},{
                '_id':0,
                'id':1,
                'serviceProvider':1,
                })
        encounter_df = pd.json_normalize(encounter_data)

        columns_to_drop = ["serviceProvider.display"]
        encounter_df.drop(columns_to_drop, axis=1,inplace=True)

        encounter_df['serviceProvider.reference'] = encounter_df['serviceProvider.reference'].str.replace('Organization/', '')

        organization_data = list(organization_collection.find({},{
                '_id':0,
                'id':1,
                'name':1,
                'address':1
                }))

        organization_df = pd.json_normalize(organization_data)

        left = encounter_df
        right = organization_df


        location_df = left.merge(right, left_on='serviceProvider.reference', right_on='id' )

        columns_to_drop = ["id_y"]
        location_df.drop(columns_to_drop, axis=1,inplace=True)
        location_df.rename(columns={"id_x": "encounter_id"}, inplace=True)
        location_df.rename(columns={"serviceProvider.reference": "organization_id"}, inplace=True)

        left = immunization_df
        right = location_df


        new_immunization_df = left.merge(right, left_on='encounter.reference', right_on='encounter_id' )

        immunization_new_df = pd.DataFrame({
            'organization_id': new_immunization_df['organization_id'],
            'organization_name': new_immunization_df['name'],
            'state_id': new_immunization_df['address'].apply(lambda x: x[0]['extension'][0]['valueCodeableConcept']['coding'][0]['code'] if x else ''),
            'state_name': new_immunization_df['address'].apply(lambda x: x[0]['extension'][0]['valueCodeableConcept']['coding'][0]['display'] if x else ''),
            'occurrence_date': new_immunization_df['occurrenceDateTime'],
            'vaccine_code': new_immunization_df['vaccine_code'],
            'vaccine_name': new_immunization_df['vaccine_name'],
            'dose': new_immunization_df['dose'],
        })

        immunization_row_counts = immunization_new_df.value_counts().reset_index(name='count')

        sorted_immunization_new_df = immunization_row_counts.sort_values(by='occurrence_date')

        sorted_immunization_new_df.reset_index(drop=True, inplace=True)

        table_name = "vaccine_administered_summary"
        conn = connection
        cursor = conn.cursor()

        try:
        
            # Step 6: Insert data from the DataFrame to the table
            for _, row in sorted_immunization_new_df.iterrows():
                insert_query = f"""
                INSERT INTO {table_name} (organization_id, organization_name,state_id,state_name,occurrence_date,vaccine_code,vaccine_name,dose,total_user)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                values = (
                    row['organization_id'],
                    row['organization_name'],
                    row['state_id'],
                    row['state_name'],
                    row['occurrence_date'],
                    row['vaccine_code'],
                    row['vaccine_name'],
                    row['dose'],
                    row['count'],
                    
                )
                cursor.execute(insert_query, values)
                conn.commit()

            print("Data inserted successfully!")
            telegram_msg(msg='Done update vaccine_administered_summary for date '+yesterday_string, conf=TOKEN_BOT_CHANNEL)


        except Exception as e:
            print(f"Error: {e}")

        finally:
            # Step 7: Close the connection
            cursor.close()
            conn.close()

# get_vaccine_administered_update()
