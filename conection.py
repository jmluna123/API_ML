import pandas as pd
import pymysql
from sshtunnel import SSHTunnelForwarder
import ConfigParser

config = ConfigParser.ConfigParser()
config.read("config.ini")

ssh_username = config['DEFAULT']['ssh_user']
ssh_password = config['DEFAULT']['ssh_pwd']
ssh_host = config['DEFAULT']['ssh_host']
ssh_port = config['DEFAULT']['ssh_port']

database_username = config['DEFAULT']['db_user']
database_password = config['DEFAULT']['db_pwd']
database_name = config['DEFAULT']['db_name']
database_host = config['DEFAULT']['db_host']
database_port = config['DEFAULT']['db_post']


def open_ssh_tunnel():
    global tunnel
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_username,
        ssh_password=ssh_password,
        remote_bind_address=(database_host, database_port),
    )

    tunnel.start()


def mysql_connect():
    global connection
    connection = pymysql.connect(
        host="127.0.0.1",
        user=database_username,
        passwd=database_password,
        db=database_name,
        port=tunnel.local_bind_port,
    )


def run_query(sql):
    return pd.read_sql_query(sql, connection)


def disconect():
    connection.close()
    tunnel.close


def get_data(date):
    try:    
        open_ssh_tunnel()
        mysql_connect()
        df = run_query(
            "SELECT * FROM prec.consumption WHERE sensor_id=82 AND dtm > '{}';".format(date)
        )
        df.to_csv("tmp.csv")
        disconect()
    except:
        df = pd.read_csv("tmp.csv")