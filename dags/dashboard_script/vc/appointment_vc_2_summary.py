import pandas as pd
import psycopg2
import pymongo
import urllib
from dashboard_script.connection import * 
from dashboard_script.function import *

def get_appointment_vc_data():
    connection =  get_postgreql_connection()
    client = get_mongodb_connection()

    # dbname = client['TestDb']
    dbname = client['TestProdDb']
    organization_collection = dbname['Organization']
    appointment_collection = dbname['Appointment']
    HealthcareService_collection = dbname['HealthcareService']
    location_collection = dbname['Location']

    conn = None

    appointment_data = list(appointment_collection.find({'serviceCategory.coding.code':'VR',"specialty": { '$exists': 1 }},{'_id':0,'id':1,'meta':1,'start':1,'participant':1,'specialty':1,'status':1}).sort('start',1))

    appointment_df = pd.json_normalize(appointment_data)

    def extract_healthcare_service_reference(participant_list):
        for participant in participant_list:
            if participant['actor']['type'] == 'HealthcareService':
                return participant['actor']['reference']
        return ''

    appointment_df['participant'] = appointment_df['participant'].apply(extract_healthcare_service_reference)

    appointment_df['participant'] = appointment_df['participant'].str.replace('HealthcareService/', '')

    healthcareservice_data = HealthcareService_collection.find({},{'_id':0,'id':1,'providedBy':1})

    healthcareservice_df = pd.json_normalize(healthcareservice_data)

    healthcareservice_df['providedBy.reference'] = healthcareservice_df['providedBy.reference'].str.replace('Organization/', '')

    organization_data = list(organization_collection.find({},{
            '_id':0,
            'id':1,
            'name':1,
            'address':1
            }))

    organization_df = pd.json_normalize(organization_data)

    left = healthcareservice_df
    right = organization_df


    location_df = left.merge(right, left_on='providedBy.reference', right_on='id' )

    new_location_df = location_df.drop("providedBy.reference", axis=1)
    new_location_df.rename(columns={"id_x": "healthcare_id"}, inplace=True)
    new_location_df.rename(columns={"id_y": "organization_id"}, inplace=True)

    left = appointment_df
    right = new_location_df


    new_appoinment_df = left.merge(right, left_on='participant', right_on='healthcare_id' )
    new_appoinment_df.drop("participant", axis=1, inplace=True)

    columns_to_convert = ['start', 'meta.lastUpdated']

    for col in columns_to_convert:
        # Convert 'encounter_date' to datetime format with specified input format and utc=True
        new_appoinment_df[col] = pd.to_datetime(new_appoinment_df[col], format='%Y-%m-%dT%H:%M:%S%z', utc=True)

        # Change the format to 'YYYY-MM-DD'
        new_appoinment_df[col] = new_appoinment_df[col].dt.strftime('%Y-%m-%d')

    def source_function(source_list):
        for entry in source_list:
            if 'cprc' in source_list:
                return '' 'CPRC'
            elif 'provider' in source_list:
                return '' 'PROVIDER'
            elif 'bbis' in source_list:
                return '' 'BBIS'
            else:
                return '' 'BBIS'
        return ''

    new_appoinment_df['source'] = new_appoinment_df['meta.source'].apply(source_function)

    columns_to_drop = ["meta.source", "meta.profile"]
    new_appoinment_df.drop(columns_to_drop, axis=1,inplace=True)

    new_appoinment_df['specialty'] = new_appoinment_df['specialty'].apply(lambda x: x[0]['coding'][0]['display'] if x else '')

    appoinment_new_df = pd.DataFrame({
        'organization_id': new_appoinment_df['organization_id'],
        'organization_name': new_appoinment_df['name'],
        'appointment_date': new_appoinment_df['start'],
        'state_id': new_appoinment_df['address'].apply(lambda x: x[0]['extension'][0]['valueCodeableConcept']['coding'][0]['code'] if x else ''),
        'state_name': new_appoinment_df['address'].apply(lambda x: x[0]['extension'][0]['valueCodeableConcept']['coding'][0]['display'] if x else ''),
        'specialty': new_appoinment_df['specialty'],
        'status': new_appoinment_df['status'],
        'source': new_appoinment_df['source']

    })

    replacement_dict = {
        'arrived': 'complete',
        'fulfilled':'complete',
        'booked':'incomplete',
        'cancelled':'incomplete',
        'noshow':'incomplete',
        }
    appoinment_new_df['status'] = appoinment_new_df['status'].replace(replacement_dict)


    appoinment_row_counts = appoinment_new_df.value_counts().reset_index(name='count')

    pivot_df = appoinment_row_counts.pivot_table(index=['organization_id', 'organization_name', 'appointment_date', 'state_id', 'state_name','specialty','source'], columns='status', values='count', fill_value=0)

    # Reset the index to turn the MultiIndex into regular columns
    pivot_df.reset_index(inplace=True)

    # Add a 'total' column by summing up the 'in-progress' and 'finished' columns
    pivot_df['total_appointment'] = pivot_df['complete'] + pivot_df['incomplete']

    # Optional: Reorder the columns if desired
    pivot_df = pivot_df[['organization_id', 'organization_name','state_id', 'state_name', 'appointment_date','specialty','source', 'incomplete',  'complete', 'total_appointment']]



    table_name = "appointment_vc_summary"
    conn = connection
    cursor = conn.cursor()

    sql = "truncate table appointment_vc_summary restart identity;"

    try:
        # execute the INSERT statement
        cursor.execute(sql)
        # commit the changes to the database
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    try:
    
        # Step 6: Insert data from the DataFrame to the table
        for _, row in pivot_df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (organization_id, organization_name,state_id,state_name,appointment_date,specialty,source,incomplete,complete,total_appointment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            values = (
                row['organization_id'],
                row['organization_name'],
                row['state_id'],
                row['state_name'],
                row['appointment_date'],
                row['specialty'],
                row['source'],
                row['incomplete'],
                row['complete'],
                row['total_appointment']
            )
            cursor.execute(insert_query, values)
            conn.commit()

        print("Data inserted successfully!")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Step 7: Close the connection
        cursor.close()
        conn.close()

# get_encounter_vc_data()
