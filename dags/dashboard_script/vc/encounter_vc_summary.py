import psycopg2
import pymongo
import urllib
from dateutil import parser
from function import *
from connection import * 

def get_encounter_vc_data():
    connection =  get_postgreql_connection()
    client = get_mongodb_connection()

    dbname = client['TestProdDb']
    organization_collection = dbname['Organization']
    practitioner_collection = dbname['Practitioner']
    enconter_collection = dbname['Encounter']

    conn = None
    sql = "truncate table encounter_vc_summary restart identity;"

    try:
        # connect to the PostgreSQL database
        conn = connection
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)



    table_summary = []
    organization_data = list(organization_collection.find({},{
        'id':1,
        'name':1,
        'address':1
        }))
    id_list = []
    name_list = []
    state_id_list= []
    state_name_list= []

    for x in organization_data:
        id_list.append(x['id'])
        name_list.append(x['name'])
        try:
            state_id_list.append(x['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['code'])
            state_name_list.append(x['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['display'])
        except (Exception, psycopg2.Error) as error:
            state_id_list.append("")
            state_name_list.append("")
    conn = None

    for index, id in enumerate(id_list):
            encounter_data = list(enconter_collection.find({'serviceProvider.reference':'Organization/'+id,'class.code':'VR','meta.source':{'$regex':'^http://person'}},{'period':1,'meta':1}).sort('period.start',1))
    
            encounter_count = 0
            organization_name = name_list[index]
            state_id = state_id_list[index]
            state_name = state_name_list[index]
            start_date = None
            encounter_date_list_check = []
            encounter_date_list = []
        
            if encounter_data:
                
                # encounter_count = len(encounter_data)
                for x in encounter_data:
                    en_date = x['period']['start']
                    en_date = date_string_convert_to_date(en_date)
                    encounter_date_list.append(en_date)
                    
                for x in encounter_data:
                    start_date = x['period']['start'] 
                    source_link = x['meta']['source']
                    source = ''
                    
                    if 'cprc' in source_link:
                        source = 'CPRC'
                    elif 'provider' in source_link:
                        source = 'PROVIDER'
                    elif 'bbis' in source_link:
                        source = 'BBIS'
                    else:
                        source = 'BBIS'

                    # encounter_count = enconter_collection.count_documents({'serviceProvider.reference':'Organization/'+id,'period':start_date})
                    str_date = parser.parse(start_date)
                    str_date = str_date.strftime('%Y-%m-%d')
                    start_date = date_string_convert_to_date(start_date)
                    encounter_count = encounter_date_list.count(start_date)

                    if start_date not in encounter_date_list_check:
                        encounter_date_list_check.append(start_date)

                        sql = "INSERT INTO encounter_vc_summary(organization_id,encounter_date,organization_name,total_encounter,state_id,state_name,source) VALUES(%s,%s,%s,%s,%s,%s,%s)"
                    
                        try:
                            # connect to the PostgreSQL database
                            conn = connection
                            # create a new cursor
                            cur = conn.cursor()
                            # execute the INSERT statement
                            cur.execute(sql,(id,start_date,organization_name,encounter_count,state_id,state_name,source))
                            # commit the changes to the database
                            conn.commit()
                            # close communication with the database
                            cur.close()
                        except (Exception, psycopg2.DatabaseError) as error:
                            print(error)
    if conn is not None:
            conn.close()

    print('done update encounter summary')

get_encounter_vc_data()
                            


