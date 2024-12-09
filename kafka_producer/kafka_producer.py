# import os
# import time
# import random
# import json
# from kafka import KafkaProducer
# import time
# import pandas as pd
# # KAFKA_BOOTSTRAP_SERVERS = "localhost:1234"
# KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# KAFKA_TOPIC_TEST = os.environ.get("KAFKA_TOPIC_TEST", "test")
# KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "7.3.1")
# producer = KafkaProducer(
#     bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],    
#     api_version=KAFKA_API_VERSION,
# )
# # csv_file = "reddit_vm.csv"
# csv_file = "ecommerce_1000_Oct_2019.csv"

# def delivery_report(err, msg):
#     if err is not None:
#         print(f"Delivery failed for record {msg.key()}: {err}")
#     else:
#         print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# data = pd.read_csv(csv_file)

# # Đảm bảo cột timestamp là kiểu datetime
# data['event_time'] = pd.to_datetime(data['event_time'])

# # Sắp xếp dữ liệu theo timestamp (nếu chưa được sắp xếp)
# data = data.sort_values(by='event_time')

# # Gửi từng dòng dữ liệu đến Kafka
# # topic = "your_topic_name"
# previous_time = None

# for index, row in data.iterrows():
#     # Chuyển dữ liệu sang chuỗi JSON
#     record_key = str(index)
#     record_value = row.to_json()
#     # print(record_value)

#     # Gửi bản ghi đến Kafka
#     # producer.produce(
#     #     topic=topic,
#     #     key=record_key,
#     #     value=record_value,
#     #     callback=delivery_report
#     # )
#     producer.send(
#         KAFKA_TOPIC_TEST,
#         record_value.encode("utf-8"),
#     )
#     producer.flush()  # Gửi dữ liệu ngay lập tức

#     # Giả lập độ trễ dựa trên timestamp
#     if previous_time is not None:
#         delay = (row['event_time'] - previous_time).total_seconds()
#         if delay > 0:
#             time.sleep(delay)
#             # time.sleep(0)


#     previous_time = row['event_time']

#     print(previous_time)
#     print("Message sent!!!!!!!!!!!!")

# # print("Streaming complete!")


import os
import time
import logging
import signal
import sys
import pandas as pd
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaProducer")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC_TEST = os.environ.get("KAFKA_TOPIC_TEST", "test")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "7.3.1")
CSV_FILE = os.getenv("CSV_FILE", "ecommerce_1000_Oct_2019.csv")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    api_version=KAFKA_API_VERSION,
)

# Graceful shutdown
def shutdown_producer(signal, frame):
    logger.info("Shutting down producer...")
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_producer)
signal.signal(signal.SIGTERM, shutdown_producer)

# Load and process CSV file
data = pd.read_csv(CSV_FILE)
data['event_time'] = pd.to_datetime(data['event_time'])
data = data.sort_values(by='event_time')

# Produce messages to Kafka
previous_time = None
batch_size = 100
records = []

for index, row in data.iterrows():
    record_key = str(index)
    record_value = row.to_json()
    future = producer.send(KAFKA_TOPIC_TEST, record_value.encode("utf-8"))
    records.append(future)

    # Simulate delay based on timestamp
    if previous_time is not None:
        delay = (row['event_time'] - previous_time).total_seconds()
        if delay > 0:
            time.sleep(delay)

    previous_time = row['event_time']

    # Flush in batches
    if len(records) >= batch_size:
        for record in records:
            try:
                record.get(timeout=10)
            except Exception as e:
                logger.error(f"Error sending record: {e}")
        records.clear()

# Final flush
producer.flush()
logger.info("Streaming complete!")
