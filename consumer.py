from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import json
import concurrent.futures
import argparse
import os
import logging

# GET INPUTS FROM ENVIRONMENT VARIABLES
# Kafka configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", 'user-login')  # Kafka topic
INSIGHTS_TOPIC = os.getenv("INSIGHTS_TOPIC", 'insights_topic')  # Insights topic
LOG_LEVEL = os.getenv("LOG_LEVEL", 'INFO')  # Log level

# Initialize variables for message count
message_count = 0
map_users_devices = {}

# Create multiple Kafka consumers in the same consumer group.
# Different conumser instances will consume from different partitions of the same topic.
def create_consumers(num_consumers = 1, consumer_conf = {}):
    consumers = []
    for i in range(num_consumers):
        consumer_instance = Consumer(consumer_conf)
        consumers.append(consumer_instance)
    return consumers

# Subscribe consumers to the Kafka topic
def subscribe_consumers(consumers, topic=KAFKA_TOPIC, insights_topic=INSIGHTS_TOPIC):
    for consumer in consumers:
        consumer.subscribe([topic])


# Run all consumers in parallel.
# This is useful when you want to run multiple consumers in parallel.
def run_all_consumers(consumers, num_consumers = 1, enable_auto_commit = True, insights_topic=INSIGHTS_TOPIC, broker='localhost:9092'):
    # create a thread pool with num consumer threads
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_consumers+1)
    
    # All consumer consume messages in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_consumers) as executor:
        # submit tasks and collect futures
        futures = [executor.submit(consume_messages,consumer, enable_auto_commit=enable_auto_commit, insights_topic=insights_topic, broker=broker) for consumer in consumers]
        concurrent.futures.wait(futures)

def create_new_topic(topic=INSIGHTS_TOPIC, broker='localhost:9092'):
    admin = AdminClient({'bootstrap.servers': broker})
    new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
    admin.create_topics([new_topic])

def create_new_messages(message, insights_topic="insights_topic", broker="localhost:9092"):
    conf = {'bootstrap.servers': broker}
    producer = Producer(conf)
    device_type = message.get("device_type", "UNKNOWN")
    timestamp = message["timestamp"]

    if device_type not in map_users_devices:
        map_users_devices[device_type] = 1
    else:
        map_users_devices[device_type] += 1
    
    total_device_till = map_users_devices[device_type]
    new_message = {
        "device_type": device_type,
        "timestamp": timestamp,
        "total_device_till": total_device_till
    }

    new_message_str = json.dumps(new_message)
    producer.produce(insights_topic, new_message_str.encode('utf-8'))
    logging.debug(f"Produced: {new_message_str}")
    producer.flush()
    
def consume_messages(consumer, enable_auto_commit=True, insights_topic='insights_topic', broker='localhost:9092'):
    logging.info("Starting consumer")
    try:
        while True:
            # Poll for a message
            msg = consumer.poll(1.0)
            # print("HERE consumer new")
            if msg is None:
                continue
            if msg.error():
                # Handle errors
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.error(f"Message consuming error - Reached end of partition {msg.partition()} offset {msg.offset()}")
                else:
                    logging.error(f"Message consuming error - Error: {msg.error()}")
            else:
                # Process the message
                msg_str = msg.value().decode('utf-8')
                logging.debug(f"Message consumed : {msg_str}")
                message = json.loads(msg_str)

                # Gain insights from this message on the number of users using a particular device type
                # Produce the insights to a new topic
                create_new_messages(message, insights_topic=insights_topic, broker=broker)

                if(not enable_auto_commit):
                    # Commit the offset if auto commit is disabled
                    try:
                        consumer.commit()
                    except KafkaException as e:
                        logging.error(f"Failed to commit message offsets - {e}")
    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer to clean up resources
        consumer.close()


def __main__():
    logging.basicConfig(filename='pipeline_log_file', level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
    parser = argparse.ArgumentParser()

    parser.add_argument('--num_consumers', type=int, default=1, help='Number of consumers to run in parallel')
    parser.add_argument('--consumer_conf', default="consumer_conf.json", help='Consumer config JSON file')

    # Parse arguments
    num_consumers = parser.parse_args().num_consumers
    consumer_conf_file = parser.parse_args().consumer_conf

    # Parse json file
    if(consumer_conf_file):
        with open(consumer_conf_file) as f:
            consumer_conf = json.load(f)


    # Check if auto commit is set in the consumer config else it is set to True by default (property of librdkafka kafka consumer)
    enable_auto_commit = consumer_conf.get('enable.auto.commit', True)
    broker = consumer_conf.get ('bootstrap.servers', 'localhost:9092')
    # Create new topic for insights
    create_new_topic('insights_topic', broker=broker)

    # Create consumers
    consumers = create_consumers(num_consumers, consumer_conf=consumer_conf)

    # Subscribe consumers to the Kafka topic
    subscribe_consumers(consumers)

    # Run all consumers
    run_all_consumers(consumers, num_consumers=num_consumers, enable_auto_commit=enable_auto_commit, broker=broker)


if __name__ == "__main__":
    __main__()