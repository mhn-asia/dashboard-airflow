import pandas as pd
import psycopg2
import pymongo
import urllib
import json
from dateutil.parser import parse
from datetime import datetime,date, timedelta

# connection_string = 'mongodb://localhost:27017/'
connection_string = 'mongodb://Nexus:'+ urllib.parse.quote("P@ssw0rd") +'@ac-wndusca-shard-00-00.h34npgh.mongodb.net:27017,ac-wndusca-shard-00-01.h34npgh.mongodb.net:27017,ac-wndusca-shard-00-02.h34npgh.mongodb.net:27017/?ssl=true&replicaSet=atlas-6kuxme-shard-0&authSource=admin&retryWrites=true&w=majority'
client = pymongo.MongoClient(connection_string, 27017)

dbname = client['uatDb']
organization_collection = dbname['Organization']
patient_collection = dbname['Patient']

yesterday = date.today() - timedelta(days=1)
# yesterday_string = yesterday.strftime("%Y-%m-%d")
yesterday_string = '2023-08-23'

conn = None

pd.set_option('display.max_colwidth', None)

              
patient_data = list(patient_collection.find({'meta.lastUpdated': {'$regex': yesterday_string}}, {'id':1, '_id':0, 'extension': 1,'meta.lastUpdated':1, 'managingOrganization.reference':1}).sort('meta.lastUpdated',1))
patient_df = pd.json_normalize(patient_data)

def extract_extension(extension_list):
    if isinstance(extension_list, list):
        for ext in extension_list:
            if isinstance(ext, dict) and ext.get('url') == 'http://fhir.hie.moh.gov.my/StructureDefinition/system-rating-my-core':
                return ext
    return None

patient_df['dateExtract'] = patient_df['extension'].apply(extract_extension)

def extract_extension(extension_list):
    if isinstance(extension_list, list):
        for ext in extension_list:
            if isinstance(ext, dict) and ext.get('url') == 'http://fhir.hie.moh.gov.my/StructureDefinition/system-rating-my-core':
                return ext
    return None

patient_df['extension'] = patient_df['extension'].apply(extract_extension)

patient_df['managingOrganization.reference'] = patient_df['managingOrganization.reference'].str.replace('Organization/','')

organization_data = list(organization_collection.find({},{
        '_id':0,
        'id':1,
        'name':1,
        'address':1
        }))


excludeId = ['HIE', 'PH', 'MPIS', 'BBIS']
organization_df = pd.json_normalize(organization_data)
organization_df = organization_df[~organization_df['id'].isin(excludeId)]

left = patient_df
right = organization_df


merge_df = left.merge(right, left_on='managingOrganization.reference', right_on='id' )
merge_df = merge_df.drop("managingOrganization.reference", axis=1)
merge_df.rename(columns={"id_y": "organization_id"}, inplace=True)

patient_new_df = pd.DataFrame(merge_df)

def extract_date_from_dict(value_dict):
    if isinstance(value_dict, dict) and 'value' in value_dict:
        return value_dict['value']
    else:
        return None

# Convert only non-None dateExtract values to datetime objects using parse
patient_new_df['meta.lastUpdated'] = patient_new_df['meta.lastUpdated'].apply(parse)
patient_new_df['dateExtract'] = patient_new_df['dateExtract'].apply(extract_date_from_dict)
patient_new_df['dateExtract'] = patient_new_df['dateExtract'].apply(lambda x: parse(x) if x is not None else None)

# Define a custom function to merge the dates
def merge_dates(row):
    if row['dateExtract'] is not None:
        return row['dateExtract']
    else:
        return row['meta.lastUpdated']

# Apply the custom function to create the merged column
patient_new_df['mergedDate'] = patient_new_df.apply(merge_dates, axis=1)

def extract_rating_code(extensions):
    # print('X1',extensions)
    try:
        if extensions is None:
            return 0

        # Check if the 'extension' list is empty
        if not extensions:
            return 0

        if extensions['url'] == "http://fhir.hie.moh.gov.my/StructureDefinition/system-rating-my-core":
            extensions2 = extensions['extension'][0]['valueCodeableConcept']['coding'][0]['code']
            # print ("X2",extensions2)
            if extensions2:
                return extensions2
            else:
                return 0
    except:
        return 0
    
def extract_date(extensions):
    # print('X1',extensions)
    try:
        if extensions is None:
            datee = patient_df['meta.lastUpdated']
            return 0

        # Check if the 'extension' list is empty
        if not extensions:
            return 0

        if extensions['url'] == "http://fhir.hie.moh.gov.my/StructureDefinition/system-rating-my-core":
            extensions2 = extensions['extension'][1]['valueDateTime']
            # print ("X2",extensions2)
            if extensions2:
                return extensions2
            else:
                return 0
    except:
        return 0
    
    


patient_new_df2 = pd.DataFrame({
    'organization_id': patient_new_df['organization_id'],
    'organization_name': patient_new_df['name'],
    'date': patient_new_df['mergedDate'],
    'rating': patient_new_df['extension'].apply(extract_rating_code),
    'state_id': patient_new_df['address'].apply(lambda x: x[0]['extension'][0]['valueCodeableConcept']['coding'][0]['code'] if x and x[0]['extension'] and 'valueCodeableConcept' in x[0]['extension'][0] and 'coding' in x[0]['extension'][0]['valueCodeableConcept'] and x[0]['extension'][0]['valueCodeableConcept']['coding'] else ''),
    'state_name': patient_new_df['address'].apply(lambda x: x[0]['extension'][0]['valueCodeableConcept']['coding'][0]['display'] if x and x[0]['extension'] and 'valueCodeableConcept' in x[0]['extension'][0] and 'coding' in x[0]['extension'][0]['valueCodeableConcept'] and x[0]['extension'][0]['valueCodeableConcept']['coding'] else ''),
})

patient_new_df2['date'] = pd.to_datetime(patient_new_df2['date'], format='%Y-%m-%dT%H:%M:%S%z', utc=True)
patient_new_df2['date'] = patient_new_df2['date'].dt.strftime('%Y-%m-%d')

patient_new_df2_counts = patient_new_df2.value_counts().reset_index(name='ratingCount')
pivot_df = patient_new_df2_counts.pivot_table(index=['organization_id', 'organization_name','date','state_id','state_name','rating','ratingCount'])

pivot_df.reset_index(inplace=True)

pivot_df = pivot_df[['organization_id', 'organization_name','date','state_id','state_name','rating','ratingCount']]


# # conn = psycopg2.connect(
# #     database="dashboard",
# #     host="10.29.209.55",
# #     options="-c search_path=legacy,golf",
# #     user="mhnusr",
# #     password="mHnY0uS3r456&890",
# #     port="5444"
# #     )

# conn = psycopg2.connect(
#     database="staging",
#     host="103.91.65.22",
#     options="-c search_path=legacy,golf",
#     user="mhnusr",
#     password="mHnY0uS3r456&890",
#     port="5093"
#     )

# table_name = "system_rating_summary"
# cursor = conn.cursor()

# sql = "truncate table system_rating_summary restart identity;"

# try:
#     # execute the INSERT statement
#     cursor.execute(sql)
#     # commit the changes to the database
#     conn.commit()
# except (Exception, psycopg2.DatabaseError) as error:
#     print(error)

# try:
#     for _, row in pivot_df.iterrows():
#         insert_query = f"""
#         INSERT INTO {table_name} (organization_id, organization_name,date,state_id,state_name,rating,ratingCount)
#         VALUES (%s, %s, %s, %s, %s, %s, %s);
#         """
#         values = (
#             row['organization_id'],
#             row['organization_name'],
#             row['date'],
#             row['state_id'],
#             row['state_name'],
#             row['rating'],
#             row['ratingCount'],
#         )
#         cursor.execute(insert_query, values)
#         conn.commit()

#     print("Data inserted successfully!")

# except Exception as e:
#     print(f"Error: {e}")

# finally:
#     # Step 7: Close the connection
#     cursor.close()
#     conn.close()    