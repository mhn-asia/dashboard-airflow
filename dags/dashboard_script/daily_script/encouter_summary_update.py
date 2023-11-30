import psycopg2
from dateutil import parser 
from function import *
from connection import *
from datetime import date, timedelta
from constants import TOKEN_BOT_CHANNEL
from helper import telegram_msg
from rekod_kesihatan.encounter_table_summary import encounter_summary_table

def get_encounter_update():
    # counter = 0
    connection =  get_postgreql_connection()
    client = get_mongodb_connection()

    dbname = client['TestProdDb']
    organization_collection = dbname['Organization']
    enconter_collection = dbname['Encounter']
    yesterday = date.today() - timedelta(days=1)
    yesterday_string = yesterday.strftime("%Y-%m-%d")
    # yesterday_string = '2023-04-17'
    
    # delete data base on today date
    sql = "DELETE FROM encounter_summary WHERE encounter_date = %s ",(yesterday_string,)

    try:
        # connect to the PostgreSQL database
        conn = connection
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(*sql)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)




    # get distict value for organization id
    id_str = []
    for testy in enconter_collection.distinct('serviceProvider.reference'):
        parts = testy.split('/')
        id_str.append(parts[1])
        
    organization_data = list(organization_collection.find({"id": {"$in": id_str}},{
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
        state_id_list.append(x['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['code'])
        state_name_list.append(x['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['display'])

    # conn = None

    for index, id in enumerate(id_list):
        encounter_data = list(enconter_collection.find({'serviceProvider.reference':'Organization/'+id, 'period.start':{'$regex':'^'+yesterday_string}},{'period':1,'meta':1}).sort('period.start',1))
        encounter_count = 0
        organization_name = name_list[index]
        state_id = state_id_list[index]
        state_name = state_name_list[index]
        start_date = None
        encounter_date_list_check = []
        encounter_date_list = []

        if encounter_data:
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
                
                appoinment_count = enconter_collection.count_documents({'serviceProvider.reference':'Organization/'+id,'period.start':{'$regex':'^'+str_date},'appointment':{'$exists':1}})
                walking_count = encounter_count - appoinment_count
                if start_date not in encounter_date_list_check:
                    encounter_date_list_check.append(start_date)
                    # counter=counter+1

                    sql = "INSERT INTO encounter_summary(organization_id,encounter_date,organization_name,total_encounter,state_id,state_name,total_walkin,total_appointment,source) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                
                    try:
                        # connect to the PostgreSQL database
                        conn = connection
                        # create a new cursor
                        cur = conn.cursor()
                        # execute the INSERT statement
                        cur.execute(sql,(id,start_date,organization_name,encounter_count,state_id,state_name,walking_count,appoinment_count,source))
                        # commit the changes to the database
                        conn.commit()
                        # close communication with the database
                        cur.close()
                    except (Exception, psycopg2.DatabaseError) as error:
                        print(error)
    if conn is not None:
            conn.close()

    # encounter_summary_table()
    
    telegram_msg(msg='Done update encounter summary for date '+yesterday_string, conf=TOKEN_BOT_CHANNEL)
                            

# get_encounter_update()