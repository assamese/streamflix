'''
Event Streamer Script

This script consumes messages from a Kafka topic and writes them to a PostgreSQL database.

Dependencies:
- kafka-python: For consuming messages from Kafka.
- psycopg2: For interacting with the PostgreSQL database.

Configuration:
- Kafka:
  - TOPIC_NAME: Name of the Kafka topic to consume messages from.
  - KAFKA_BOOTSTRAP_SERVERS: Kafka bootstrap server address.
  - CONSUMER_CLIENT_ID: Kafka consumer client ID.
  - CONSUMER_GROUP_ID: Kafka consumer group ID.
  - SSL_CAFILE, SSL_CERTFILE, SSL_KEYFILE: SSL configuration for secure Kafka connection.
- PostgreSQL:
  - POSTGRES_HOST: PostgreSQL server host.
  - POSTGRES_PORT: PostgreSQL server port.
  - POSTGRES_DB: Name of the PostgreSQL database.
  - POSTGRES_USER: PostgreSQL username.
  - POSTGRES_PASSWORD: PostgreSQL password.
  - POSTGRES_TABLE: Name of the table to write data to.
  - POSTGRES_SSL_MODE: SSL mode for PostgreSQL connection.

Functionality:
1. Connects to a Kafka topic and polls messages.
2. Decodes and parses the messages as JSON.
3. Writes the parsed messages to a PostgreSQL table.

Usage:
- Ensure all dependencies are installed (`pip install kafka-python psycopg2`).
- Update the configuration variables with appropriate values.
- Run the script to start consuming messages and writing to the database.

Note:
- Ensure the PostgreSQL table schema matches the fields in the Kafka messages.
- Handle sensitive information (e.g., passwords) securely in production environments.

'''
import json
from kafka import KafkaConsumer
import psycopg2
import os

# Kafka configuration
TOPIC_NAME = "trumid_streamflix_topic1"
KAFKA_BOOTSTRAP_SERVERS = "kafka-223cf11a-llmtravel.b.aivencloud.com:21734"
CONSUMER_CLIENT_ID = "CONSUMER_CLIENT_ID"
CONSUMER_GROUP_ID = "CONSUMER_GROUP_ID"
SSL_CAFILE = "../secrets/ca.pem"
SSL_CERTFILE = "../secrets/service.cert"
SSL_KEYFILE = "../secrets/service.key"

# PostgreSQL configuration
POSTGRES_HOST = "pg-20c7c62-llmtravel.i.aivencloud.com" 
POSTGRES_PORT = 21732
POSTGRES_DB = "defaultdb"  
POSTGRES_USER = "avnadmin"  
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")  # Read PostgreSQL password from an environment variable
if not POSTGRES_PASSWORD:
    raise EnvironmentError("Error: The environment variable 'POSTGRES_PASSWORD' is not set.")
POSTGRES_TABLE = "events_table"  # Name of the table to write to
POSTGRES_SSL_MODE = "require"  # Enforce SSL connection

def write_to_postgres(messages):
    """Writes a list of Kafka messages (as dictionaries) to PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            sslmode=POSTGRES_SSL_MODE
        )
        cur = conn.cursor()
        sql = f"""
            INSERT INTO {POSTGRES_TABLE} (event_id, event_ts, user_id, content_id, event_type, playback_position, device)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        values = [
            (
                msg['event_id'],
                msg['event_ts'],
                msg['user_id'],
                msg['content_id'],
                msg['event_type'],
                msg['playback_position'],
                msg['device']
            )
            for msg in messages
        ]
        cur.executemany(sql, values)
        conn.commit()
        print(f"Successfully wrote {len(messages)} messages to PostgreSQL.")
    except psycopg2.Error as e:
        print(f"Error writing to PostgreSQL: {e}")
        conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    client_id=CONSUMER_CLIENT_ID,
    group_id=CONSUMER_GROUP_ID,
    security_protocol="SSL",
    ssl_cafile=SSL_CAFILE,
    ssl_certfile=SSL_CERTFILE,
    ssl_keyfile=SSL_KEYFILE,
    auto_offset_reset='latest'  # Or 'earliest' depending on your needs
)

while True:
    messages_buffer = []
    messages_polled = consumer.poll(timeout_ms=5000, max_records=1000)  # Poll for up to 1000 messages or 5 seconds

    if not messages_polled:
        print("No messages received in the last 5 seconds. Continuing to poll...")
        continue

    for partition_messages in messages_polled.values():
        for message in partition_messages:
            try:
                decoded_message = message.value.decode('utf-8')
                json_message = json.loads(decoded_message)
                messages_buffer.append(json_message)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}, message: {message.value.decode('utf-8')}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")

    if messages_buffer:
        write_to_postgres(messages_buffer)

