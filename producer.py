from kafka import KafkaProducer
import threading
import time
import json
import random


class Producer(threading.Thread):
    daemon = True

    def run(self):
        local_server = "prdserver2:6667"
        dev_server = "localhost:9092"
        producer = KafkaProducer(bootstrap_servers=[local_server],
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        percent = "%"

        while True:
            t = time.localtime()
            current_time = time.strftime("%H:%M:%S", t)
            log = producer.send('prod_L3', {"user_time": str(current_time), "alt": str(random.randrange(60, 100, 3))+percent})
            print(log)
            time.sleep(10)


call = Producer()
call.run()