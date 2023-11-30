from connection import *
from function import *
from constants import TOKEN_BOT_CHANNEL
from helper import telegram_msg

def user_summary_table():
    connection =  get_postgreql_connection()

    try:
        sql = "SELECT DISTINCT organization_id FROM user_summary"           

        # connect to the PostgreSQL database
        conn = connection
        # create a new cursor
        cur = conn.cursor()
        # execute the DISTINCT statement
        cur.execute(sql)
        facility_id = cur.fetchall()

        reset_sql = "TRUNCATE TABLE golf.user_table_summary RESTART IDENTITY"
        cur.execute(reset_sql)

        for row in facility_id:
            try:
                sql = "SELECT DISTINCT role_id FROM user_summary WHERE organization_id= %s",(row)           

                # execute the select statement
                cur.execute(*sql)
                # commit the changes to the database
                role_id = cur.fetchall()

                for role in role_id:
                    sql = "select * from user_summary WHERE organization_id = %s AND role_id = %s",(row,role)
                    # execute the select statement
                    cur.execute(*sql)
                    # commit the changes to the database
                    role_rekod = cur.fetchall()
                    total_role_number = 0

                    for row1 in role_rekod:
                        total_role_number += row1[7]
                    
                    sql = "INSERT INTO user_table_summary(organization_id,state_id,organization_name,state_name,role_id,role_name,total_user) VALUES(%s,%s,%s,%s,%s,%s,%s)"
                    try:
                        # execute the INSERT statement
                        cur.execute(sql,(row1[1],row1[2],row1[3],row1[4],row1[5],row1[6],total_role_number))
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

user_summary_table()