import os
import sys
from dotenv import load_dotenv
import jaydebeapi

load_dotenv()


def load_data():
    remote_filepath = os.getenv("PSS_PATH")
    local_server = os.getenv("SKORUZ_IP")
    local_filepath = os.getenv("SKORUZ_PATH")
    remote_server = os.getenv("SKORUZ_WEBSITE")
    server_port = os.getenv("SKORUZ_PORT")
    server_database = os.getenv("SKORUZ_DB")

    filename = sys.argv[1]
    filename = os.path.splitext(filename)[0]
    data_dir = os.path.join(os.getcwd(), "datafile/" + sys.argv[1])
    f = open(data_dir, "r")
    datastore = f.read()
    try:
        conn = jaydebeapi.connect("com.simba.hive.jdbc41.HS2Driver",
                                  "jdbc:hive2://" + remote_server + ":" + server_port +
                                  "/" + server_database, {'user': "hive", 'password': ""}, remote_filepath)
        curs = conn.cursor()
        curs.execute('CREATE TABLE ' + filename + ' (str STRING) STORED AS ORC')
        curs.execute('INSERT INTO ' + filename + ' VALUES (\'' + datastore + '\')')
        conn.close()
    except Exception:
        print("Error")


load_data()
