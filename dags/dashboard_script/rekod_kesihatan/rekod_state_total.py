from connection import *
from function import *
from constants import TOKEN_BOT_CHANNEL
from helper import telegram_msg

def rekod_state_total():
    connection =  get_postgreql_connection()

    try:
        conn = None
        query1 = "SELECT DISTINCT state_id FROM register_summary"      

        # connect to the PostgreSQL database
        conn = connection
        # create a new cursor
        cur = conn.cursor()
        # execute the DISTINCT statement
        cur.execute(query1)
        # commit the changes to the database
        state_id = cur.fetchall()

        reset_sql = "TRUNCATE TABLE golf.state_total_summary RESTART IDENTITY"
        cur.execute(reset_sql)

        # get Malaysia data(total)

        sql = "SELECT SUM(total_register) FROM register_summary"           
        sql1 = "SELECT SUM(total_encounter) FROM encounter_summary"
        sql2 = "SELECT SUM(total_walkin) FROM encounter_summary" 
        sql3 = "SELECT SUM(total_appointment) FROM encounter_summary" 
        cur.execute(sql)
        total_register_data = cur.fetchone()
        total_register = tuple_to_int(total_register_data)
        cur.execute(sql1)
        total_encounter_data = cur.fetchone()
        total_encounter = tuple_to_int(total_encounter_data)
        cur.execute(sql2)
        total_walkin_data = cur.fetchone()
        total_walkin = tuple_to_int(total_walkin_data)
        cur.execute(sql3)
        total_appointment_data = cur.fetchone()
        total_appointment = tuple_to_int(total_appointment_data)

        malay_sql = "INSERT INTO state_total_summary(state_id,state_name,total_register,total_encounter,total_walkin,total_appointment) VALUES(%s,%s,%s,%s,%s,%s)"
        cur.execute(malay_sql,(00,'Malaysia',total_register,total_encounter,total_walkin,total_appointment))
        conn.commit()


        # get state data(total)
        for row in state_id:
            
            register_sql = "select * from register_summary WHERE state_id = %s",(row)

            cur.execute(*register_sql)
            state_regis_rekod = cur.fetchall()
            state_total_register = 0

            for regis in state_regis_rekod:
                state_total_register += regis[4]
            
            encounter_sql  = "select * from encounter_summary WHERE state_id = %s",(row)

            cur.execute(*encounter_sql)
            state_encoun_rekod = cur.fetchall()
            state_total_encounter = 0
            state_total_walkin = 0
            state_total_appointment = 0

            for row1 in state_encoun_rekod:
                state_total_encounter += row1[4]
                state_total_walkin += row1[7]
                state_total_appointment += row1[8]

            print(state_total_encounter)
            
            sql = "INSERT INTO state_total_summary(state_id,state_name,total_register,total_encounter,total_walkin,total_appointment) VALUES(%s,%s,%s,%s,%s,%s)"
            try:
                # execute the INSERT statement
                cur.execute(sql,(regis[5],regis[6],state_total_register,state_total_encounter,state_total_walkin,state_total_appointment))
                # commit the changes to the database
                conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
        
        if conn is not None:
            conn.close()

    
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)


rekod_state_total()
