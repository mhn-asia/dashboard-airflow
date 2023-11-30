import psycopg2
from dateutil import parser 
from dashboard_script.function import *
from dashboard_script.connection import *
from datetime import datetime,date, timedelta
from dashboard_script.constants import TOKEN_BOT_CHANNEL
from dashboard_script.helper import telegram_msg
# from dashboard_script.rekod_kesihatan.encounter_table_summary import encounter_summary_table

def get_encounter2_update(**kwargs):
    # counter = 0
    connection =  get_postgreql_connection()
    client = get_mongodb_connection()

    dbname = client['TestProdDb']
    organization_collection = dbname['Organization']
    enconter_collection = dbname['Encounter']
    execution_date = kwargs['execution_date']
    yesterday = execution_date - timedelta(days=1)
    yesterday_string = yesterday.strftime("%Y-%m-%d")
    # yesterday_string = '2023-07-24'
    
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


# /////////////////////////////////////////////////////

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

    for index, id in enumerate(id_list):
        encounter_data = list(enconter_collection.find({'serviceProvider.reference':'Organization/'+id,'meta.lastUpdated':{'$regex':'^'+yesterday_string}},{'period':1,'meta':1}).sort('meta.lastUpdated',1))
        encounter_count = 0
        organization_name = name_list[index]
        state_id = state_id_list[index]
        state_name = state_name_list[index]
        start_date = None
        encounter_date_list_check = []
        encounter_date_list = []
        unique_enc_date = []

        if encounter_data:

            # dapatkan semua period date from last update 
            for x in encounter_data:
                en_date = x['period']['start']
                en_date = date_string_convert_to_date(en_date)
                encounter_date_list.append(en_date)

            unique_enc_date = list(set(encounter_date_list))
            
            # new section
            link_list = ['cprc','provider','person']
            source = ''
            for enc_source in link_list:
                source_check = enc_source
                if source_check == 'cprc':
                    source = 'CPRC'
                else: 
                    source = 'PROVIDER'

                for update_date in unique_enc_date:
                    date_str = update_date.strftime('%Y-%m-%d %H:%M:%S')
                    dt_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                    str_date = dt_obj.strftime('%Y-%m-%d')
                    encounter_count = enconter_collection.count_documents({'serviceProvider.reference':'Organization/'+id,'period.start':{'$regex':'^'+str_date},'meta.source':{'$regex':'^http://'+enc_source}})
                    appoinment_count = enconter_collection.count_documents({'serviceProvider.reference':'Organization/'+id,'period.start':{'$regex':'^'+str_date},'appointment':{'$exists':1},'meta.source':{'$regex':'^http://'+enc_source}})
                    walking_count = encounter_count - appoinment_count
                    # print('organization_name',organization_name)
                    # print('encounter_count',encounter_count)
                    if encounter_count != 0:
                        sql = "INSERT INTO encounter_summary(organization_id,encounter_date,organization_name,total_encounter,state_id,state_name,total_walkin,total_appointment,source) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (organization_id,encounter_date,source) DO UPDATE SET total_encounter = EXCLUDED.total_encounter, total_walkin = EXCLUDED.total_walkin, total_appointment = EXCLUDED.total_appointment "

                        try:
                            # connect to the PostgreSQL database
                            conn = connection
                            # create a new cursor
                            cur = conn.cursor()
                            # execute the INSERT statement
                            cur.execute(sql,(id,str_date,organization_name,encounter_count,state_id,state_name,walking_count,appoinment_count,source))
                            # commit the changes to the database
                            conn.commit()
                            # close communication with the database
                            cur.close()
                        except (Exception, psycopg2.DatabaseError) as error:
                            print(error)


                # else:
                #     source = 'PROVIDER'
                #     encounter_count = enconter_collection.count_documents({'serviceProvider.reference':'Organization/'+id,'period.start':{'$regex':'^'+yesterday_string},'meta.source':{'$regex':'^http://'+enc_source}})
                #     appoinment_count = enconter_collection.count_documents({'serviceProvider.reference':'Organization/'+id,'period.start':{'$regex':'^'+yesterday_string},'appointment':{'$exists':1},'meta.source':{'$regex':'^http://'+enc_source}})
                #     walking_count = encounter_count - appoinment_count
                #     if encounter_count != 0:
                #         sql = "INSERT INTO encounter_summary(organization_id,encounter_date,organization_name,total_encounter,state_id,state_name,total_walkin,total_appointment,source) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    
                #         try:
                #             # connect to the PostgreSQL database
                #             conn = connection
                #             # create a new cursor
                #             cur = conn.cursor()
                #             # execute the INSERT statement
                #             cur.execute(sql,(id,yesterday_string,organization_name,encounter_count,state_id,state_name,walking_count,appoinment_count,source))
                #             # commit the changes to the database
                #             conn.commit()
                #             # close communication with the database
                #             cur.close()
                #         except (Exception, psycopg2.DatabaseError) as error:
                #             print(error)
    if conn is not None:
        conn.close()

    telegram_msg(msg='Done update encounter summary for date '+yesterday_string, conf=TOKEN_BOT_CHANNEL)
    # print('Done update encounter summary for date '+yesterday_string)                            

# get_encounter2_update()