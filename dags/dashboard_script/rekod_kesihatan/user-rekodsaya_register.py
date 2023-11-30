from connection import*
from function import*
import pandas as pd

def rekod_saya():
    connection1 =  get_postgreql_connection()
    conn1 = connection1

    df = pd.read_sql_query("select date,total_register,total_activate,total_nonactivate from rekod_saya", conn1)
    df.to_parquet('C:/Users/MHN0094/Development/Github Dev/test_example/rekod_saya_data.parquet')

    

rekod_saya()