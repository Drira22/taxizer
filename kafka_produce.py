# import pandas as pd
# df = pd.read_csv('C:/P2M/Data/MTA-Bus-Time_.2014-08-01.txt', delimiter='\t')
# df.to_csv('C:/P2M/Data/MTA-Bus-Time_.2014-08-01.csv', index=False)

import subprocess
import os
from time import sleep
import csv
import json
from kafka import KafkaProducer

class KafkaManager:
    def __init__(self, topic_name,kafka_bootstrap_servers):
        self.topic_name = topic_name
        self.kafka_bootstrap_servers=kafka_bootstrap_servers

    def start_zookeeper(self):
        subprocess.Popen(["gnome-terminal", "--", "zookeeper-server-start.sh", "config/zookeeper.properties"], start_new_session=True)

    def start_kafka_server(self):
        subprocess.Popen(["gnome-terminal", "--", "kafka-server-start.bat", "config/server.properties"], start_new_session=True)

    def check_topic_exists(self):
        result = subprocess.run(["kafka-topics.sh", "--list", "--bootstrap-server", self.kafka_bootstrap_servers], capture_output=True, text=True)
        existing_topics = result.stdout.split('\n')
        return self.topic_name in existing_topics

    def create_topic(self):
        subprocess.run(["kafka-topics.sh", "--create", "--topic", self.topic_name, "--bootstrap-server", self.kafka_bootstrap_servers, "--replication-factor", "1", "--partitions", "1"])

    def start_consumer(self):
        subprocess.Popen(["kafka-console-consumer.sh", "--topic", self.topic_name, "--from-beginning", "--bootstrap-server", self.kafka_bootstrap_servers])

    def run_producer(self, file_path):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        with open(file_path) as file:
            reader = csv.DictReader(file)
            for row in reader:
                message = json.dumps(row).encode()
                producer.send(self.topic_name, value=message)
                sleep(0.1)

    def start(self, file_path):
        os.chdir("kafka/kafka-src")
        if not self.check_topic_exists():
            self.create_topic()
        self.start_consumer()
        sleep(5)
        self.run_producer(file_path)




# from kafka import KafkaConsumer
#
# consumer= KafkaConsumer(
#     'read_data_test',
#     bootstrap_servers='localhost:9092',
#     auto_offset_reset='earliest'
# )
#
# for message in consumer:
#     print(message.value.decode())
