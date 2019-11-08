from kafka import KafkaProducer
import threading
import time
import json
import random
import os
from dotenv import load_dotenv
import sys

load_dotenv()


class Producer(threading.Thread):
    daemon = True

    def run(self):
        local_kafka_server = os.getenv("SKORUZ_KAFKA_SERVER")
        dev_server = "localhost:9092"
        topic_name = sys.argv[1]
        producer = KafkaProducer(bootstrap_servers=[local_kafka_server],
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        percent = "%"

        while True:
            t = time.localtime()
            current_time = time.strftime("%H:%M:%S", t)
            log = producer.send(topic_name,
                                {"user_time": str(current_time), "humidity": str(random.randrange(60, 100, 3)) + percent
                                    , "alt": str(random.randrange(60, 1000, 3)) + 'ft'})
            print(log)
            time.sleep(10)


call = Producer()
call.run()
