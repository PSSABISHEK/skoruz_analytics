from kafka.admin import KafkaAdminClient, NewTopic
import sys
import os
import random
import json
import jaydebeapi


# CREATE EXTERNAL TABLE IF NOT EXISTS (user_time STRING, alt STRING) STORED AS ORC

def create_topic():
    local_server = "prdserver2:6667"
    dev_server = "localhost:9092"
    admin_client = KafkaAdminClient(bootstrap_servers=local_server, client_id='test')
    filename = sys.argv[1]
    filename = os.path.splitext(filename)[0]
    while True:
        topic_list = [NewTopic(name=filename, num_partitions=1, replication_factor=1)]
        try:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            break
        except Exception as error:
            filename = filename + "_" + str(random.randrange(1, 1000, 3))
    create_table(filename)
    return filename


def create_table(table_name):
    remote_filepath = r"F:\Internship\Skoruz\HDFS\Simba_Hive_JDBC\SIMBAHiveJDBC41.jar"
    local_server = "172.16.1.30"
    local_filepath = r"/home/prd_user/SIMBAHiveJDBC41.jar"
    remote_server = "india.skoruz.com"
    f = open(sys.argv[1], "r")
    datastore = json.load(f)
    values = []
    i = len(datastore.keys())
    j = 0
    for key in datastore.keys():
        values.append(key)
        values.append("STRING")
        if j < i - 1:
            values.append(";")
        j += 1
    values = ",".join(values)
    values = values.replace(',', ' ')
    values = values.replace(';', ',')
    conn = jaydebeapi.connect("com.simba.hive.jdbc41.HS2Driver", "jdbc:hive2://" + local_server + ":10000/copy",
                              {'user': "hive", 'password': ""}, local_filepath)
    curs = conn.cursor()
    curs.execute('CREATE TABLE ' + table_name + '(' + values + ') STORED AS ORC')
    conn.close()


print(create_topic())
