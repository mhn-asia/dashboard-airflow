from connection import *
from function import *
import pandas as pd

def rekod_saya_summary():
    connection =  get_postgreql_connection()
    try:
        sql = "SELECT SUM(total_register) FROM register_summary"           
        sql1 = "SELECT SUM(total_encounter) FROM encounter_summary"
        sql2 = "SELECT SUM(total_walkin) FROM encounter_summary" 
        sql3 = "SELECT SUM(total_appointment) FROM encounter_summary" 
        # connect to the PostgreSQL database
        conn = connection
        # create a new cursor
        cur = conn.cursor()
        # execute the DISTINCT statement
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

        data = [{'total_register': total_register, 'total_encounter': total_encounter,'total_walkin':total_walkin,'total_appointment':total_appointment} ]
        df = pd.DataFrame(data)
        df.to_parquet('D:/example/rekod_saya_summary.parquet')
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

rekod_saya_summary()