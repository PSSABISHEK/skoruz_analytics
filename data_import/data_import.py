import os
import sys

import jaydebeapi


def load_data():
    remote_filepath = r"F:\Internship\Skoruz\HDFS\Simba_Hive_JDBC\SIMBAHiveJDBC41.jar"
    local_server = "172.16.1.30"
    local_filepath = r"/home/prd_user/SIMBAHiveJDBC41.jar"
    remote_server = "india.skoruz.com"

    filename = sys.argv[1]
    filename = os.path.splitext(filename)[0]
    data_dir = os.path.join(os.getcwd(), "datafile/" + sys.argv[1])
    f = open(data_dir, "r")
    datastore = f.read()
    try:
        conn = jaydebeapi.connect("com.simba.hive.jdbc41.HS2Driver", "jdbc:hive2://" + local_server + ":10000/copy",
                                  {'user': "hive", 'password': ""}, local_filepath)
        curs = conn.cursor()
        curs.execute('CREATE TABLE ' + filename + ' (str STRING) STORED AS ORC')
        curs.execute('INSERT INTO ' + filename + ' VALUES (\'' + datastore + '\')')
    except Exception:
        print("Error")
    finally:
        conn.close()


load_data()
