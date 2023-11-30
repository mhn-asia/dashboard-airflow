import psycopg2
import pymongo
import urllib
from dateutil import parser
from function import *
from connection import * 
import pandas as pd

def get_appointment_vc_data():
    connection =  get_postgreql_connection()
    client = get_mongodb_connection()

    dbname = client['TestDb']
    organization_collection = dbname['Organization']
    appointment_collection = dbname['Appointment']
    HealthcareService_collection = dbname['HealthcareService']
    location_collection = dbname['Location']

    conn =None
    # sql = "truncate table appointment_vc_summary restart identity;"

    # try:
    #     # connect to the PostgreSQL database
    #     conn = connection
    #     # create a new cursor
    #     cur = conn.cursor()
    #     # execute the INSERT statement
    #     cur.execute(sql)
    #     # commit the changes to the database
    #     conn.commit()
    #     # close communication with the database
    #     cur.close()
    # except (Exception, psycopg2.DatabaseError) as error:
    #     print(error)

    appointment_data = list(appointment_collection.find({'serviceCategory.coding.code':'VR',"specialty": { '$exists': 1 }},{'meta':1,'start':1,'participant':1,'specialty':1}).sort('start',1))
    encounter_date_list = []
    encounter_date_list_check = []
    column_names = ['organization_id', 'appointment_date', 'organization_name','state_id','state_name','specialty','source']
    df = pd.DataFrame(columns=column_names)

    for appointment in appointment_data:
        start_date = appointment['start'] 
        source_link = appointment['meta']['source']
        source = ''
        
        if 'cprc' in source_link:
            source = 'CPRC'
        elif 'provider' in source_link:
            source = 'PROVIDER'
        elif 'bbis' in source_link:
            source = 'BBIS'
        else:
            source = 'BBIS'

        str_date = parser.parse(start_date)
        str_date = str_date.strftime('%Y-%m-%d')
        start_date = date_string_convert_to_date(start_date)
        specialty = appointment['specialty'][0]['coding'][0]['display']

        healthcare_ref = appointment['participant'][0]['actor']['reference']
        print(healthcare_ref)
        healthcare_ref = healthcare_ref.split('/')
        if healthcare_ref[0] == 'Location':
            location_data = location_collection.find_one({'id':healthcare_ref[1]},{'managingOrganization':1})
            org_ref = location_data['managingOrganization']['reference']
            org_ref = org_ref.split('/')
            org_id = org_ref[1]
            organization_data = organization_collection.find_one({'id':org_ref[1]},{'name':1,'address':1})
            org_name =  organization_data['name']
            state_code = organization_data['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['code']
            state_name = organization_data['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['display']

          
        elif healthcare_ref[0] == 'HealthcareService':
            healthcareService_data = HealthcareService_collection.find_one({'id':healthcare_ref[1]},{'providedBy':1})
            org_ref = healthcareService_data['providedBy']['reference']
            org_ref = org_ref.split('/')
            org_id = org_ref[1]
            organization_data = organization_collection.find_one({'id':org_ref[1]},{'name':1,'address':1})
            org_name =  organization_data['name']
            state_code = organization_data['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['code']
            state_name = organization_data['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['display']
        
        else:
            continue
       
        new_row = pd.Series({'organization_id':org_id, 'appointment_date':start_date, 'organization_name':org_name,'state_id':state_code,'state_name':state_name,'specialty':specialty,'source':source})
        df.loc[len(df)] = new_row


    print('df',df)
    counts = df.value_counts().reset_index(name='total_appointment')
    print('count',counts)

    for index, row in counts.iterrows():
        # Get the values from each column
        col1 = row['organization_id']
        col2 = row['appointment_date']
        col3 = row['organization_name']
        col4 = row['total_appointment']
        col5 = row['state_id']
        col6 = row['state_name']
        col7 = row['specialty']
        col8 = row['source']
        
        # Do something with the values
        sql = "INSERT INTO appointment_vc_summary(organization_id,appointment_date,organization_name,total_appointment,state_id,state_name,specialty,source) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
                
        try:
            # connect to the PostgreSQL database
            conn = connection
            # create a new cursor
            cur = conn.cursor()
            # execute the INSERT statement
            cur.execute(sql,(col1,col2,col3,col4,col5,col6,col7,col8))
            # commit the changes to the database
            conn.commit()
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    if conn is not None:
            conn.close()

    # print('done update encounter summary')

# get_appointment_vc_data()
                            


