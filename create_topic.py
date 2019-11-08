from kafka.admin import KafkaAdminClient, NewTopic
import sys
import os
import random
import json
import jaydebeapi
from dotenv import load_dotenv

load_dotenv()


def create_topic():
    local_kafka_server = os.getenv("SKORUZ_KAFKA_SERVER")
    remote_server = "localhost:9092"
    admin_client = KafkaAdminClient(bootstrap_servers=local_kafka_server, client_id='test.py')
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
    remote_filepath = os.getenv("PSS_PATH")
    local_server = os.getenv("SKORUZ_IP")
    local_filepath = os.getenv("SKORUZ_PATH")
    remote_server = os.getenv("SKORUZ_WEBSITE")
    server_port = os.getenv("SKORUZ_PORT")
    server_database = os.getenv("SKORUZ_DB")
    conn = jaydebeapi.connect("com.simba.hive.jdbc41.HS2Driver",
                              "jdbc:hive2://" + local_server + ":" + server_port +
                              "/" + server_database, {'user': "hive", 'password': ""}, local_filepath)
    curs = conn.cursor()
    curs.execute('CREATE TABLE ' + table_name + '( str STRING ) STORED AS ORC')
    conn.close()


print(create_topic())
