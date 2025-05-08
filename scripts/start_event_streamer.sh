python3 ../Python/event_streamer.py > foo.consumer 2>&1 &
python3 ../Python/aiven_kafka_producer.py clicks_file > foo.producer 2>&1 &
