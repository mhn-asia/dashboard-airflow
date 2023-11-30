import psycopg2
from dateutil import parser 
from function import *
from connection import *
from datetime import date, timedelta
from constants import TOKEN_BOT_CHANNEL
from helper import telegram_msg
from rekod_kesihatan.register_table_summary import register_summary_table
from phis_facility import phis_facility,phis_facility_list

def register_data_update():
    connection =  get_postgreql_connection()
    client = get_mongodb_connection()

    dbname = client['TestProdDb']
    organization_collection = dbname['Organization']
    patient_collection = dbname['Patient']

    yesterday = date.today() - timedelta(days=1)
    yesterday_string = yesterday.strftime("%Y-%m-%d")
    # yesterday_string = '2023-08-08'

    # # delete data base on today date
    sql = "DELETE FROM register_summary WHERE register_date = %s ",(yesterday_string,)

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
    for testy in patient_collection.distinct('managingOrganization.reference'):
        parts = testy.split('/')
        id_str.append(parts[1])

    organization_data = list(organization_collection.find({"id": {"$in": id_str,"$ne": "PHIS"}},{
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

    print(state_id_list)
    # conn = None


    for index, id in enumerate(id_list):

        register_data = list(patient_collection.find({'$and':[{'managingOrganization.reference':'Organization/'+id},{'managingOrganization.reference':{'$not':{'$regex':'^PHIS'}}},{'meta.tag.display':{'$ne':'Golden Record'}}, {'meta.lastUpdated': {'$regex':'^'+yesterday_string}}]},{'meta':1}).sort('meta.lastUpdated',1))

        encounter_count = 0
        organization_name = name_list[index]
        state_id = state_id_list[index]
        state_name = state_name_list[index]
        start_date = None
        role_id_list_check = []
        register_date_list_check = []
        register_date_list = []

        if register_data:
                
            # encounter_count = len(encounter_data)
            for x in register_data:
                en_date = x['meta']['lastUpdated']
                en_date = date_string_convert_to_date(en_date)
                register_date_list.append(en_date)
                
            for x in register_data:
                start_date = x['meta']['lastUpdated']
                source_link = x['meta']['source']
                if 'cprc' in source_link:
                    source = 'CPRC'
                elif 'provider' in source_link:
                    source = 'PROVIDER'
                elif 'bbis' in source_link:
                    source = 'BBIS'
                else:
                    source = 'BBIS'   
                # encounter_count = enconter_collection.count_documents({'serviceProvider.reference':'Organization/'+id,'period':start_date})
                start_date = date_string_convert_to_date(start_date)
                encounter_count = register_date_list.count(start_date)
                # nanti buat appointment
                if start_date not in register_date_list_check:
                    register_date_list_check.append(start_date)

                    sql = "INSERT INTO register_summary(organization_id,register_date,organization_name,total_register,state_id,state_name,source) VALUES(%s,%s,%s,%s,%s,%s,%s)"
                
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


    for data_facility_id in phis_facility_list:
        register_data_phis = patient_collection.find_one({'id':{'$regex': '^PHIS-'+data_facility_id},'meta.tag.display':{'$ne':'Golden Record'}, 'meta.lastUpdated': {'$regex':'^'+yesterday_string}},{'id':1,'meta':1})
        if register_data_phis:
            phis_register_date_list_check = []
            facility_prefix = data_facility_id
            phis_org_id = phis_facility[facility_prefix]
            start_date = register_data_phis['meta']['lastUpdated']
            start_date = date_string_convert_to_date(start_date)
            date_string = start_date.strftime("%Y-%m-%d")
            phis_encounter_count = patient_collection.count_documents({'id':{'$regex': '^PHIS-'+data_facility_id},'meta.lastUpdated':{'$regex': '^' + date_string}})

                    


            organization_data = organization_collection.find_one({"id":phis_org_id},{
                'id':1,
                'name':1,
                'address':1
            })

            organization_name = organization_data['name']
            state_id = organization_data['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['code']
            state_name = organization_data['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['display']

            source = 'PHIS'

            if start_date not in phis_register_date_list_check:
                phis_register_date_list_check.append(start_date)

                sql = "INSERT INTO register_summary(organization_id,register_date,organization_name,total_register,state_id,state_name,source) VALUES(%s,%s,%s,%s,%s,%s,%s)"
            
                try:
                    # connect to the PostgreSQL database
                    conn = connection
                    # create a new cursor
                    cur = conn.cursor()
                    # execute the INSERT statement
                    cur.execute(sql,(phis_org_id,start_date,organization_name,phis_encounter_count,state_id,state_name,source))
                    # commit the changes to the database
                    conn.commit()
                    # close communication with the database
                    # cur.close()
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)

    if conn is not None:
            conn.close()

    telegram_msg(msg='Done update register summary for date '+yesterday_string, conf=TOKEN_BOT_CHANNEL)
    # print('Done update register summary for date '+yesterday_string)                            

# register_data_update()