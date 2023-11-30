from pymongo import ReplaceOne
import requests
from requests.auth import HTTPBasicAuth
import json
import re
import time
from datetime import date, timedelta,datetime
from dashboard_script.connection import *
from dashboard_script.constants import TOKEN_BOT_CHANNEL
from dashboard_script.helper import telegram_msg

client = get_mongodb_connection()

# production
def get_fhir_data_update(**kwargs):
    counter = 0
    dbname = client['TestProdDb']
    url = "https://fhir.rekodkesihatan.moh.gov.my/baseR4/$export"
    auth = HTTPBasicAuth('zulfahmi', 'hUu$437T')
    
    # dbname = client['TestDb']
    # url = "https://smilecdr.mhn.asia/baseR4/$export"
    # yesterday = date.today() - timedelta(days=1)
    # iso_date = yesterday.strftime('%Y-%m-%dT%H:%M:%S')
    # # iso_date ='2023-07-18T00:00:00'

    
    # Get the current date
    # current_date = datetime.now()
    execution_date = kwargs['execution_date']

    # Subtract one day to get the previous day's date
    previous_date = execution_date - timedelta(days=1)

    # Hardcode the time and timezone offset
    iso_date = previous_date.strftime('%Y-%m-%dT00:00:01+08:00')
    # iso_date = '2023-10-31T00:00:01+08:00'
    print(iso_date)
    valueString =  "AllergyIntolerance, Appointment, Composition, Condition, Consent, Encounter, HealthcareService, Immunization, ImmunizationRecommendation, List, Observation, Organization, Patient, Practitioner, PractitionerRole, ServiceRequest"
    # valueString = 'Patient'

    headers = {
        "prefer": "respond-async"
        }
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {
            "name": "_outputFormat",
            "valueString": "application/fhir+ndjson"
            },
            {
            "name": "_type",
            "valueString": valueString
            },
            {
            "name": "_since",
            "valueInstant": iso_date
            }
        ]
    }

    telegram_msg(msg='start get fhir data', conf=TOKEN_BOT_CHANNEL)

    response=requests.post(url, headers=headers, json=body, auth=auth)
    # response=requests.post(url, headers=headers, json=body)

    content_location = response.headers['Content-Location']
    response1 = requests.get(content_location, auth=auth)
    # response1 = requests.get(content_location)

    gap = 5 * 60
    stop_time = 3*60*60
    start_time = time.time()

    while response1.status_code != 200:
        response1 = requests.get(content_location, auth=auth)
        # response1 = requests.get(content_location)  


         # Check if the time difference exceeds the specified gap
        if time.time() - start_time >= stop_time:
            # telegram_msg(msg='Build fail', conf=TOKEN_BOT_CHANNEL)
            # print('Build fail')
            break
        time.sleep(gap)   


    try:
        resp_dict = response1.json()

        # telegram_msg(msg='Request been build', conf=TOKEN_BOT_CHANNEL)
        # print('Request been build')

        # response1 = requests.get(content_location,auth=auth).text
        output_data = resp_dict['output']
        # for update & create data into db

        for index, item in enumerate(output_data):
            
            output_type = output_data[index]['type']
            collection = dbname[output_type]
            output_url = output_data[index]['url']
           
            data_request = requests.get(output_url, auth=auth ,stream=True)
            # data_request = requests.get(output_url, stream=True)

            # print('data_request',type(data_request))

            operations = [] 

            for line in data_request.iter_lines():
                try:
                    json_data = json.loads(line)
                    id = json_data['id']

                    # Find the existing document with the same ID
                    existing_doc = collection.find_one({'id': id})
                    
                    if existing_doc:
                        # Preserve the 'recordedDate' field from the existing document
                        recorded_date = existing_doc.get('recordedDate')
                        json_data['recordedDate'] = recorded_date
                        # Create a ReplaceOne operation that replaces the entire document
                        operation = ReplaceOne({'id': id}, json_data, upsert=True)
                    else:
                        # Create a new document
                        operation = ReplaceOne({'id': id}, json_data, upsert=True)
                    
                    operations.append(operation)

                    # Perform bulk_write when there's a certain number of operations stored (e.g., 100)
                    if len(operations) >= 10000:
                        collection.bulk_write(operations)
                        operations.clear()
                        print('Done with 100 data items')
                        
                except json.JSONDecodeError as e:
                    print('Error parsing JSON:', str(e))

                yesterday = date.today() - timedelta(days=1)
                yesterday_string = yesterday.strftime("%Y-%m-%d")
                # yesterday_string = '2023-06-10'
            
            # Handle the remaining operations, if any
            if operations:
                collection.bulk_write(operations)
                print(f'Done with {len(operations)} data items')



        # for index, item in enumerate(output_data):

        #     # try:
            
        #         output_type = output_data[index]['type']
        #         collection = dbname[output_type]
        #         output_url = output_data[index]['url']
                
        #         data_request = requests.get(output_url,auth=auth, stream=True)

        #         # inset to db
        #         for line in data_request.iter_lines():
        #             if line:
        #                 try:
        #                     json_data = json.loads(line)
        #                     id = json_data['id']
        #                     collection.replace_one(
        #                         {'id':id},
        #                         json_data,
        #                         upsert= True 
        #                         )
        #                     counter=counter+1
        #                     # print('done one data')
        #                 except json.JSONDecodeError as e:
        #                     print('Error parsing JSON:', str(e))

               


        #     # except:
        #     #     print('fail')
        #     #     continue
    except json.JSONDecodeError:
        print('Response could not be serialized')

   
    telegram_msg(msg='Done update fhir data for date '+ yesterday_string +' with count of '+str(counter), conf=TOKEN_BOT_CHANNEL)
    # print('Done update fhir data for date')

# kalau nk run file ni sahaja
# get_fhir_data_update()