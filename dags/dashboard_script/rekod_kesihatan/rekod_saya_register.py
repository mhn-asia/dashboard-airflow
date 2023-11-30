from connection import *
from function import *

def rekod_saya_register():
    connection =  get_postgreql_connection()
    try:
        register_date_sql = "SELECT DISTINCT created_date FROM rekod_saya_summary ORDER BY created_date ASC"
        # connect to the PostgreSQL database
        conn = connection
        # create a new cursor
        cur = conn.cursor()
        # execute the DISTINCT statement
        cur.execute(register_date_sql)
        # commit the changes to the database
        register_date = cur.fetchall()


        

        for cre_date in register_date:
            sql_date = "select * from rekod_saya_summary WHERE created_date::timestamp = %s",(cre_date)
            cur.execute(*sql_date)
            date_filter = cur.fetchall()
            total_register = 0
            total_activate = 0
            total_nonactivate = 0
            for total in date_filter:
                total_register += total[2]
                total_activate += total[3]
                total_nonactivate += total[4]

            sql_sort = ""
            sql_insert = "INSERT INTO rekod_saya(create_date,total_register,total_activate,total_nonactivate) VALUES(%s,%s,%s,%s)"
            try:
                # execute the INSERT statement
                cur.execute(sql_insert,(cre_date,total_register, total_activate, total_nonactivate))
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

rekod_saya_register()