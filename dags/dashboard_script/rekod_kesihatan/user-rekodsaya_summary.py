from connection import*
from function import*
import pandas as pd

def rekod_saya_summary():
    connection =  get_postgreql_connection()

    try:
        sql = "SELECT SUM(register) FROM rekod_saya_summary"
        sql1 = "SELECT SUM(activated) FROM rekod_saya_summary"
        sql2 = "SELECT SUM(nonactivate) FROM rekod_saya_summary"

        conn = connection
        cur = conn.cursor()
        cur.execute(sql)
        total_register_data = cur.fetchone()
        total_register = tuple_to_int(total_register_data)
        cur.execute(sql1)
        total_activated_data = cur.fetchone()
        total_activated = tuple_to_int(total_activated_data)
        cur.execute(sql2)
        total_nonactivated_data = cur.fetchone()
        total_nonactivated = tuple_to_int(total_nonactivated_data)

        data = [{'register' : total_register, 'activated' : total_activated, 'nonactivate' : total_nonactivated} ]
        df = pd.DataFrame(data)
        df.to_parquet('C:/Users/MHN0094/Development/Github Dev/test_example/register-stat.parquet')

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

rekod_saya_summary()