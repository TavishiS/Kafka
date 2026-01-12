import os
from app.storage import append_event
from dotenv import load_dotenv

load_dotenv()

num_partitions = int(os.getenv("NUM_PARTITIONS"))

def produce_event(user_id : int, topic : str, event : str):
    partition_id = (hash(user_id) % num_partitions) # keeping no. of partitions same under each topic
    append_event(topic, partition_id, event)