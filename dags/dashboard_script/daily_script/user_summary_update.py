import psycopg2
from dashboard_script.connection import *
from dateutil import parser 
from datetime import date, timedelta
from dashboard_script.function import *
from dashboard_script.constants import TOKEN_BOT_CHANNEL
from dashboard_script.helper import telegram_msg

def get_user_data(**kwargs):
    connection = get_postgreql_connection()
    client = get_mongodb_connection()
    dbname = client['TestProdDb']
    organization_collection = dbname['Organization']
    practitionerRole_collection = dbname['PractitionerRole']

    execution_date = kwargs['execution_date']
    yesterday = execution_date - timedelta(days=1)
    yesterday_string = yesterday.strftime("%Y-%m-%d")
    # yesterday_string = '2023-07-24'

    conn = None
    # sql = "truncate table user_summary restart identity;"

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
        
    sql = "DELETE FROM user_summary WHERE last_update_date = %s ",(yesterday_string,)

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

    id_str = []
    for testy in practitionerRole_collection.distinct('organization.reference'):
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

    # unique_code_id = practitionerRole_collection.distinct( "code.coding.code" )
    # print('done get unique code')

    for index, id in enumerate(id_list):
        unique_code_id = practitionerRole_collection.distinct( "code.coding.code", { 'organization.reference':'Organization/'+id })
        if unique_code_id:
            for code in unique_code_id:
                role_data = list(practitionerRole_collection.find({'organization.reference':'Organization/'+id,'meta.lastUpdated':{'$regex':'^'+yesterday_string},'code.coding.code':code},{'code':1,'practitioner':1,'meta':1,'active':1}))
                encounter_count = 0
                organization_name = name_list[index]
                state_id = state_id_list[index]
                state_name = state_name_list[index]
                role_id_list_check = []
                datetime_check = []

                if role_data:
                    for x in role_data:
                        role_id = x['code'][0]['coding'][0]['code']
                        role_name = x['code'][0]['coding'][0]['display']
                        last_update = x['meta']['lastUpdated']
                        last_update_str = parser.parse(last_update)
                        last_update_str = last_update_str.strftime('%Y-%m-%d')
                        last_update = date_string_convert_to_date(last_update)
                        source = 'PROVIDER'
                        role_count = practitionerRole_collection.count_documents({'organization.reference':'Organization/'+id,'code.coding.code':role_id,'meta.lastUpdated':{'$regex':'^'+last_update_str}})
                        active_count = practitionerRole_collection.count_documents({'organization.reference':'Organization/'+id,'code.coding.code':role_id,'active':True,'meta.lastUpdated':{'$regex':'^'+last_update_str}})
                        if role_count != 0 and last_update_str not in datetime_check:
                            datetime_check.append(last_update_str)
                            sql = "INSERT INTO user_summary(organization_id,state_id,organization_name,state_name,role_id,role_name,total_user,active_user,last_update_date,source) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                            
                            try:
                                # connect to the PostgreSQL database
                                conn = connection
                                # create a new cursor
                                cur = conn.cursor()
                                # execute the INSERT statement
                                cur.execute(sql,(id,state_id,organization_name,state_name,role_id,role_name,role_count,active_count,last_update,source))
                                # commit the changes to the database
                                conn.commit()
                                # close communication with the database
                                cur.close()
                            except (Exception, psycopg2.DatabaseError) as error:
                                print(error)

    if conn is not None:
        conn.close()

    telegram_msg(msg='Done update user summary for date '+yesterday_string, conf=TOKEN_BOT_CHANNEL)
    # print('Done update  user summary for date '+yesterday_string)                            


# get_user_data()