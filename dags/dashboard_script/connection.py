import psycopg2
import pymongo
import urllib

def get_postgreql_connection():
# connect postgresql db staging
    connection = psycopg2.connect(database="staging",
                        host="103.91.65.22",
                        options="-c search_path=legacy,golf",
                        user="mhnusr",
                        password="mHnY0uS3r456&890",
                        port="5093")
    return connection

    # connect postgresql db localhost
    # connection = psycopg2.connect(database="summary",
    #                     host="127.0.0.1",
    #                     options="-c search_path=legacy,public",
    #                     user="postgres",
    #                     password="postgres",
    #                     port="5432")
    # return connection

    # connect postgresql db Production
    # connection = psycopg2.connect(database="dashboard",
    #                     host="10.29.209.55",
    #                     options="-c search_path=legacy,golf",
    #                     user="mhnusr",
    #                     password="mHnY0uS3r456&890",
    #                     port="5444")
    # return connection

def get_keyclock_postgreql_connection():
    connection = psycopg2.connect(database="keycloak",
                        host="10.29.209.55",
                        options="-c search_path=legacy,public",
                        user="mhnusr",
                        password="mHnY0uS3r456&890",
                        port="5444")
    return connection

def get_mongodb_connection():
    # connection_string = 'mongodb+srv://Nexus:'+ urllib.parse.quote("P@ssw0rd") +'@cluster0.h34npgh.mongodb.net/test?retryWrites=true&w=majority'
    # connection_string = 'mongodb://Nexus:'+ urllib.parse.quote("P@ssw0rd") +'@ac-wndusca-shard-00-00.h34npgh.mongodb.net:27017,ac-wndusca-shard-00-01.h34npgh.mongodb.net:27017,ac-wndusca-shard-00-02.h34npgh.mongodb.net:27017/?ssl=true&replicaSet=atlas-6kuxme-shard-0&authSource=admin&retryWrites=true&w=majority'
    # connection_string = 'mongodb://admin:password@103.91.65.22:27017/?authMechanism=DEFAULT'
    # connection_string = 'mongodb://mg_master:8wY0^K&ekxG4c4foV659C9tf@10.29.209.70:8443/?authMechanism=DEFAULT'
    connection_string = 'mongodb://localhost:27017'
    client = pymongo.MongoClient(connection_string, 27017)
    return client