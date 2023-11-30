from connection import *
from function import *

connection =  get_postgreql_connection()

register_date_sql = "SELECT DISTINCT created_date FROM rekod_saya_summary"
# connect to the PostgreSQL database
conn = connection
# create a new cursor
cur = conn.cursor()
# execute the DISTINCT statement
cur.execute(register_date_sql)
# commit the changes to the database
create_date = cur.fetchall()

state_id_sql = "SELECT DISTINCT state_name FROM rekod_saya_summary"
cur.execute(state_id_sql)
state_name = cur.fetchall()

for cre_date in create_date:
    for state in state_name:
        sql_state = "select * from rekod_saya_summary WHERE created_date::timestamp = %s and state_name = %s ",(cre_date,state)
        cur.execute(*sql_state)
        rekod_filter = cur.fetchall() 
        if rekod_filter:
            total_actived = 0
            for x in rekod_filter:
               total_actived += x[3]

            state_short = get_state_short(x[6])

            sql_insert = "INSERT INTO rekod_saya_state(state_name,state_short,create_date,total_activated) VALUES(%s,%s,%s,%s)"
            try:
                # execute the INSERT statement
                cur.execute(sql_insert,(x[6],state_short,x[1],total_actived))
                # commit the changes to the database
                conn.commit()
                # close communication with the database
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)

if conn is not None:
    conn.close()
             