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

def add_recordedDate(**kwargs):
    connection =  get_postgreql_connection()
    client = get_mongodb_connection()

    dbname = client['TestProdDb']
    organization_collection = dbname['Organization']
    patient_collection = dbname['Patient']

    execution_date = kwargs['execution_date']
    yesterday = execution_date - timedelta(days=1)
    yesterday_string = yesterday.strftime("%Y-%m-%d")
    # yesterday_string = '2023'

    conn = None

    patient_data = list(patient_collection.find({'meta.lastUpdated': {'$regex': '2023-11'}}, {'id':1, '_id':0, 'extension': 1,'meta': 1,'recordedDate':1, 'managingOrganization.reference':1}).sort('recordedDate',1))
    patient_df = pd.json_normalize(patient_data)

    # Define the updated extract_extension function
    def extract_extension(extension_list):
        if isinstance(extension_list, list):
            for ext in extension_list:
                if isinstance(ext, dict) and ext.get('url') == 'http://fhir.hie.moh.gov.my/StructureDefinition/audit-my-core':
                    if ext.get("extension") and len(ext["extension"]) > 1:
                        valueDateTime = ext["extension"][1].get('valueDateTime')
                        if valueDateTime >= "2022-10":
                            return valueDateTime
        return None

    # Assuming 'patient_df' is a DataFrame with an 'extension' column containing the extension data
    patient_df['audit'] = patient_df['extension'].apply(extract_extension)

    def update_recorded_date(row):
        # Check if 'recordedDate' is null
        if pd.isnull(row['recordedDate']):
            # Check if 'audit' column is not null
            if not pd.isnull(row['audit']):
                return row['audit']
            # Check if 'meta.lastUpdated' is not null
            if not pd.isnull(row['meta.lastUpdated']):
                return row['meta.lastUpdated']
        return row['recordedDate']

    # Apply the custom function to update the 'recordedDate' column
    patient_df['recordedDate'] = patient_df.apply(update_recorded_date, axis=1)

    # Iterate over your DataFrame
    for _, row in patient_df.iterrows():
        id_value = row['id']
        recorded_date = row['recordedDate']

        # Update the document in the 'Patient' collection based on the 'id' field
        patient_collection.update_one({'id': id_value}, {'$set': {'recordedDate': recorded_date}})

    # Close the MongoDB client when you're done
    client.close()

    telegram_msg(msg='done add recorded date for paitent', conf=TOKEN_BOT_CHANNEL)
