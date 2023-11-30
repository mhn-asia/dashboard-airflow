from dashboard_script.connection import *
from dashboard_script.function import *
from datetime import date, timedelta
from dashboard_script.constants import TOKEN_BOT_CHANNEL
from dashboard_script.helper import telegram_msg

def rekod_summary_update(**kwargs):
    connection =  get_keyclock_postgreql_connection()
    # today_date = date.today() 
    # today_date = today_date.strftime("%d/%m/%Y")
    execution_date = kwargs['execution_date']
    yesterday = execution_date - timedelta(days=1)
    yesterday_string = yesterday.strftime("%d/%m/%Y")
    # yesterday_string = '24/07/2023'
    try:
        sql = "select * from rekod_saya_summary where created_date = %s ",(yesterday_string,)           

        # connect to the PostgreSQL database
        conn = connection
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(*sql)
        # commit the changes to the database
        summary_rekod = cur.fetchall()

        for row in summary_rekod:
            print(row)
        
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if connection:
            cur.close()
            connection.close()

    # insert to prod db
    conn = None
    second_db_connection = get_postgreql_connection()

    # delete data base on today date
    sql = "DELETE FROM rekod_saya_summary WHERE created_date = %s ",(yesterday_string,)

    try:
        # connect to the PostgreSQL database
        conn = second_db_connection
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


    for row in summary_rekod:    
        created_date =  row[0]
        register =  row[1]
        activated  = row[2]
        nonactivate  = row[3] 
        facility_name  = row[4] 


        sql = "INSERT INTO rekod_saya_summary(created_date,register,activated,nonactivate,facility_name) VALUES(%s,%s,%s,%s,%s)"       
        try:
            # connect to the PostgreSQL database
            conn = second_db_connection
            # create a new cursor
            cur = conn.cursor()
            # execute the INSERT statement
            cur.execute(sql,(created_date,register,activated,nonactivate,facility_name))
            # commit the changes to the database
            conn.commit()
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    if conn is not None:
        conn.close()

    telegram_msg(msg='Done update rekod saya for date '+yesterday_string, conf=TOKEN_BOT_CHANNEL)
    # print('Done update rekod saya for date '+yesterday_string)                            


# rekod_summary_update()