from connection import *
from function import *
from constants import TOKEN_BOT_CHANNEL
from helper import telegram_msg

def register_summary_table():
    connection =  get_postgreql_connection()
    try:
        sql = "SELECT DISTINCT register_summary.organization_name FROM register_summary"           

        # connect to the PostgreSQL database
        conn = connection
        # create a new cursor
        cur = conn.cursor()
        # execute the DISTINCT statement
        cur.execute(sql)
        # commit the changes to the database
        facility_name = cur.fetchall()

        reset_sql = "TRUNCATE TABLE golf.register_table_summary RESTART IDENTITY"
        cur.execute(reset_sql)

        for row in facility_name:
            try:
                sql = "select * from register_summary WHERE organization_name = %s",(row)           

                # execute the select statement
                cur.execute(*sql)
                # commit the changes to the database
                summary_rekod = cur.fetchall()
                facility_total_register = 0

                for row1 in summary_rekod:
                    facility_total_register += row1[4]

                sql = "INSERT INTO register_table_summary(organization_id,organization_name,total_register,state_id,state_name,source) VALUES(%s,%s,%s,%s,%s,%s)"
                try:
                    # execute the INSERT statement
                    cur.execute(sql,(row1[1],row1[3],facility_total_register,row1[5],row1[6],row1[7]))
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

# register_summary_table()