import pandas as pd
from connection import *
from constants import TOKEN_BOT_CHANNEL
from helper import telegram_msg

def convert_to_parquet():
    # connection =  get_keyclock_postgreql_connection()
    # conn = connection
    # df = pd.read_sql_query("select * from rekod_saya_summary", conn)
    # df.to_parquet('D:/example/rekod_saya_summary.parquet')
    connection1 =  get_postgreql_connection()
    conn1 = connection1

    # today_date = date.today()
    # today_date = today_date.strftime("%Y-%m-%d")
    # today_date = '2022-12-11'

    # df = pd.read_sql_query("select * from encounter_summary", conn1)
    # df.to_parquet('D:/example/encounter_summary.parquet')
    # df = pd.read_sql_query("select * from register_summary", conn1)
    # df.to_parquet('D:/example/register_summary.parquet')
    # df = pd.read_sql_query("select * from user_summary", conn1)
    # df.to_parquet('D:/example/user_summary.parquet')
    # df = pd.read_sql_query("select * from register_table_summary", conn1)
    # df.to_parquet('D:/example/register_table_summary.parquet')
    # df = pd.read_sql_query("select * from encounter_table_summary", conn1)
    # df.to_parquet('D:/example/encounter_table_summary.parquet')
    # df = pd.read_sql_query("select * from user_table_summary", conn1)
    # df.to_parquet('D:/example/user_table_summary.parquet')
    # df = pd.read_sql_query("select * from state_register", conn1)
    # df.to_parquet('D:/example/state_register.parquet')
    # df = pd.read_sql_query("select * from state_total_summary", conn1)
    # df.to_parquet('D:/example/state_total_summary.parquet')
    # df = pd.read_sql_query("select * from rekod_saya_state", conn1)
    # df.to_parquet('D:/example/rekod_saya_state.parquet')
    df = pd.read_sql_query("select * from timeline_encounter_summary_test", conn1)
    df.to_parquet('D:/example/timeline_encounter_summary_test.parquet')

    # telegram_msg(msg='Done update encounter summary for date'+today_date, conf=TOKEN_BOT_CHANNEL)

# convert_to_parquet()