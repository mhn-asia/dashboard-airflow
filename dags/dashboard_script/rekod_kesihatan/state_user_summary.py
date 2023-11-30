import psycopg2
from dateutil import parser 
from function import *
from connection import *

connection =  get_postgreql_connection()
client = get_mongodb_connection()

dbname = client['TestProdDb']
organization_collection = dbname['Organization']
practitionerRole_collection = dbname['PractitionerRole']
conn = None

sql = "truncate table state_user_summary restart identity;"

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

organization_data = list(organization_collection.find({},{
    'id':1,
    'name':1,
    'address':1
    }))

id_list = []
name_list = []
state_id_list= []
state_name_list= []
unique_state_id = []
unique_state_name = []

# get unique state name & id
for x in organization_data:
    # id_list.append(x['id'])
    # name_list.append(x['name'])
    # state_id_list.append(x['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['code'])
    # state_name_list.append(x['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['display'])
    if x['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['code'] not in unique_state_id:
        unique_state_id.append(x['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['code'])
        unique_state_name.append(x['address'][0]['extension'][0]['valueCodeableConcept']['coding'][0]['display'])


# get unique id role
role_code =  practitionerRole_collection.distinct('code.coding.code')

for index, id in enumerate(unique_state_id):
    current_state_name = unique_state_name[index]
    state_org_data = organization_collection.find({'address.extension.valueCodeableConcept.coding.code':id})
    organization_id = []
    for item in state_org_data:
        organization_id.append('Organization/'+item['id'])

    # print('organization_id',organization_id)
    

    for z in role_code:

        state_role_data = practitionerRole_collection.find({'organization.reference':{ '$in': organization_id },'code.coding.code':z})
      
        role_id_list_check = []
        if state_role_data:
            for x in state_role_data:

                role_id = x['code'][0]['coding'][0]['code']
                role_name = x['code'][0]['coding'][0]['display']
                role_count = practitionerRole_collection.count_documents({'organization.reference':{ '$in': organization_id },'code.coding.code':z})
    
  
                if role_id not in role_id_list_check:
                    role_id_list_check.append(role_id)

                    sql = "INSERT INTO state_user_summary(state_id,state_name,role_id,role_name,total_user) VALUES(%s,%s,%s,%s,%s)"
            
                    try:
                        # connect to the PostgreSQL database
                        conn = connection
                        # create a new cursor
                        cur = conn.cursor()
                        # execute the INSERT statement
                        cur.execute(sql,(id,current_state_name,role_id,role_name,role_count))
                        # commit the changes to the database
                        conn.commit()
                        # close communication with the database
                        cur.close()
                    except (Exception, psycopg2.DatabaseError) as error:
                        print(error)

if conn is not None:
    conn.close()