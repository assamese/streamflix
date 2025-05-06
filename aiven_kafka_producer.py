import time
from kafka import KafkaProducer

FILE_TO_READ = "../data_samples/clicks_v1.json"

TOPIC_NAME = "trumid_streamflix_topic1"

producer = KafkaProducer(
    bootstrap_servers=f"kafka-223cf11a-llmtravel.b.aivencloud.com:21734",
    security_protocol="SSL",
    ssl_cafile="../secrets/ca.pem",
    ssl_certfile="../secrets/service.cert",
    ssl_keyfile="../secrets/service.key",
)

try:
    with open(FILE_TO_READ, 'r') as file:
        while True:
            line = file.readline()
            if not line:
                break
            producer.send(TOPIC_NAME, line.encode('utf-8'))
            print(f"Message sent: {line}")
            time.sleep(0.1)
except FileNotFoundError:
    print(f"Error: File not found at '{file_path}'")
except Exception as e:
    print(f"An error occurred: {e}")


producer.close()


