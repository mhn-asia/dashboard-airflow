from connection import *
from function import *
from datetime import date, timedelta, datetime

def timline_summary_table():
    connection =  get_postgreql_connection()
    try:
        encouter_date_sql = "SELECT DISTINCT encounter_date FROM encounter_summary"
        # connect to the PostgreSQL database
        conn = connection
        # create a new cursor
        cur = conn.cursor()
        # execute the DISTINCT statement
        cur.execute(encouter_date_sql)
        # commit the changes to the database
        encounter_date = cur.fetchall()

        state_id_sql = "SELECT DISTINCT state_id FROM encounter_summary"
        cur.execute(state_id_sql)
        state_id = cur.fetchall()

        for enc_date in encounter_date:
            date_string = enc_date[0]
            
            for id_state in state_id:
                try:
                    summary_rekod =[]
                    sql = "select * from encounter_summary WHERE state_id = %s and encounter_date = %s",(id_state,date_string)
                    cur.execute(*sql)
                    summary_rekod = cur.fetchall()
                    if summary_rekod:
                        state_total_encounter = 0
                        state_total_walkin = 0
                        state_total_appointment = 0

                        for row1 in summary_rekod:
                            state_total_encounter += row1[4]
                            state_total_walkin += row1[7]
                            state_total_appointment += row1[8]
                        
                        state_short = get_state_short(row1[6])

                        sql_insert = "INSERT INTO timeline_encounter_summary(state_id,state,state_short,date,total_encounter,total_walkin,total_appointment) VALUES(%s,%s,%s,%s,%s,%s,%s)"
                        try:
                            # execute the INSERT statement
                            cur.execute(sql_insert,(row1[5],row1[6],state_short,enc_date,state_total_encounter,state_total_walkin,state_total_appointment))
                            # commit the changes to the database
                            conn.commit()
                            # close communication with the database
                        except (Exception, psycopg2.DatabaseError) as error:
                            print(error)
                except (Exception, psycopg2.Error) as error:
                    print("Error while fetching data from PostgreSQL", error)
                

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        # closing database connection.
        if connection:
            cur.close()
            connection.close()

timline_summary_table()