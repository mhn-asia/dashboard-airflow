from connection import *
from function import *

def state_register_table():
    connection =  get_postgreql_connection()
    try:
        sql = "SELECT SUM(total_register) FROM register_summary"           
        sql1 = "SELECT DISTINCT state_id FROM register_summary" 
        # connect to the PostgreSQL database
        conn = connection
        # create a new cursor
        cur = conn.cursor()
        # execute the DISTINCT statement
        cur.execute(sql)
        total_register = cur.fetchone()
        my_strings = [str(x) for x in total_register]
        my_string = "".join(my_strings)
        my_int = int(my_string)        

        cur.execute(sql1)
        facility_name = cur.fetchall()
       
        for x in facility_name:
            try:
                sql = "select * from register_summary WHERE state_id = %s",(x)           

                # execute the select statement
                cur.execute(*sql)
                # commit the changes to the database
                summary_rekod = cur.fetchall()
                state_total_register = 0
                total_percentage = 0

                for row1 in summary_rekod:
                    state_total_register += row1[4]
                
                total_percentage = (state_total_register/my_int)*100
                state_short_name = get_state_short(row1[6])

                sql = "INSERT INTO state_register(state_id,state_name,state_short_name,total_register,total_register_percent) VALUES(%s,%s,%s,%s,%s)"
                try:
                    # execute the INSERT statement
                    cur.execute(sql,(row1[5],row1[6],state_short_name,state_total_register,total_percentage))
                    # commit the changes to the database
                    conn.commit()
                    # close communication with the database
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)

            except (Exception, psycopg2.Error) as error:
                print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if connection:
            cur.close()
            connection.close()

state_register_table()