from kafka import KafkaConsumer
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
KAFKA_BOOTSTRAP_SERVERS

# To consume latest messages and auto-commit offsets
bootstrap_servers = [i for i in
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS").split(" ")]
consumer = KafkaConsumer('twitter-topic',
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='latest', enable_auto_commit=False,
                         value_deserializer=lambda m: m)

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))