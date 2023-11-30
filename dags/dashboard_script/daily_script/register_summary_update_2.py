import pandas as pd
import psycopg2
import pymongo
import urllib
import json
from dashboard_script.function import *
from dashboard_script.connection import *
from datetime import date, timedelta
from dashboard_script.constants import TOKEN_BOT_CHANNEL
from dashboard_script.helper import telegram_msg
from dateutil.parser import parser
from dashboard_script.phis_facility import phis_facility,phis_facility_list
from pytz import timezone

def register_data_update(**kwargs):
    connection =  get_postgreql_connection()
    client = get_mongodb_connection()

    dbname = client['TestProdDb']
    organization_collection = dbname['Organization']
    patient_collection = dbname['Patient']

    execution_date = kwargs['execution_date']
    yesterday = execution_date - timedelta(days=1)
    yesterday_string = yesterday.strftime("%Y-%m-%d")
    # yesterday_string = '2023-10-30'

    conn = None

    patient_data = list(patient_collection.find({'meta.tag.display':{'$ne':'Golden Record'},'recordedDate':{'$regex': yesterday_string}},{'id':1,'managingOrganization.reference':1, 'meta':1, 'recordedDate':1}))

    patient_df = pd.json_normalize(patient_data)

    patient_df['managingOrganization.reference'] = patient_df['managingOrganization.reference'].str.replace('Organization/', '')

    def map_to_org_code(id_str):
        if 'PHIS' in id_str:
            parts = id_str.split('-')
            code = parts[1]  # Assuming the code is always the second part after splitting by '-'
            return phis_facility.get(code, 'Not Found')  # Default to 'Not Found' if code not in phis_facility
        else:
            return None  # Return the original value if 'PHIS' is not present in 'id'

    # Apply the function to the 'id' column and create a new 'org_code' column
    patient_df['org_code'] = patient_df['id'].apply(map_to_org_code)

    def merge_org_code(row):
        if 'PHIS' in row['managingOrganization.reference']:
            return row['org_code']
        else:
            return row['managingOrganization.reference']

    # Apply the merge function to create the updated 'managingOrg' column
    patient_df['managingOrganization.reference'] = patient_df.apply(merge_org_code, axis=1)

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
                    return 'PHIS'
                else:
                    return 'BBIS'
            return ''
        return ''

    patient_df['meta.source2'] = patient_df['meta.source'].apply(source_function)

    gmt8 = timezone('Asia/Kuala_Lumpur')

    # Convert the 'recordedDate' strings to datetime objects in GMT+8 timezone
    patient_df['recordedDate'] = pd.to_datetime(patient_df['recordedDate'], format='%Y-%m-%dT%H:%M:%S%z', utc=True).dt.tz_convert(gmt8)

    # Format the datetime as a date string
    patient_df['recordedDate'] = patient_df['recordedDate'].dt.strftime('%Y-%m-%d')

    organization_data = list(organization_collection.find({},{
            '_id':0,
            'id':1,
            'name':1,
            'address':1
            }))


    excludeId = []
    organization_df = pd.json_normalize(organization_data)
    organization_df = organization_df[~organization_df['id'].isin(excludeId)]

    left = patient_df
    right = organization_df

    merge_df = left.merge(right, left_on='managingOrganization.reference', right_on='id')

    def get_state_id(x):
        if isinstance(x, list) and x:
            extension = x[0]['extension']
            if extension and 'valueCodeableConcept' in extension[0] and 'coding' in extension[0]['valueCodeableConcept'] and extension[0]['valueCodeableConcept']['coding']:
                return extension[0]['valueCodeableConcept']['coding'][0]['code']
        return ''

    def get_state_name(x):
        if isinstance(x, list) and x:
            extension = x[0]['extension']
            if extension and 'valueCodeableConcept' in extension[0] and 'coding' in extension[0]['valueCodeableConcept'] and extension[0]['valueCodeableConcept']['coding']:
                return extension[0]['valueCodeableConcept']['coding'][0]['display']
        return ''

    register_df = pd.DataFrame({
        'organization_id': merge_df['id_y'],
        'register_date': merge_df['recordedDate'],
        'organization_name': merge_df['name'],
        'state_id': merge_df['address'].apply(get_state_id),
        'state_name': merge_df['address'].apply(get_state_name),
        'source': merge_df['meta.source2']
    })

    unique_values = register_df['organization_id'].unique()

    register_count_df = register_df.value_counts().reset_index(name='Count')

    column_sum = register_count_df['Count'].sum()


    table_name = "register_summary"
    conn = connection
    cursor = conn.cursor()

    try:
        for _, row in register_count_df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (organization_id,register_date,organization_name,total_register,state_id,state_name,source)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            values = (
                row['organization_id'],
                row['register_date'],
                row['organization_name'],
                row['Count'],
                row['state_id'],
                row['state_name'],
                row['source'],
            )
            cursor.execute(insert_query, values)
            conn.commit()

        print("Data inserted successfully!")
        telegram_msg(msg='Done update register_summary for date '+yesterday_string, conf=TOKEN_BOT_CHANNEL)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Step 7: Close the connection
        cursor.close()
        conn.close()    

# register_data_update()