from kafka import KafkaConsumer
from json import load, loads
from dotenv import load_dotenv
import sys
import os
import jaydebeapi

load_dotenv()


def run_consumer():
    remote_filepath = os.getenv("PSS_PATH")
    local_server = os.getenv("SKORUZ_IP")
    local_filepath = os.getenv("SKORUZ_PATH")
    remote_server = os.getenv("SKORUZ_WEBSITE")
    server_port = os.getenv("SKORUZ_PORT")
    server_database = os.getenv("SKORUZ_DB")
    local_kafka_server = os.getenv("SKORUZ_KAFKA_SERVER")
    remote_kafka_server = 'localhost:9092'
    topic_name = sys.argv[1]
    conn = jaydebeapi.connect("com.simba.hive.jdbc41.HS2Driver",
                              "jdbc:hive2://" + local_server + ":" + server_port +
                              "/" + server_database, {'user': "hive", 'password': ""},
                              local_filepath)
    curs = conn.cursor()
    print("Connection Established")
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[local_kafka_server],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))
    for message in consumer:
        message = message.value
        values = []
        for i in message:
            values.append(message[i])
        values = ', '.join(['"{}"'.format(value) for value in values])
        try:
            curs.execute('INSERT INTO ' + topic_name + ' VALUES (' + values + ')')
            print("Intserted : ", message)
        except Exception:
            print("Insert Error")
    conn.close()


run_consumer()
