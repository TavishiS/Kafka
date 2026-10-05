import os
import hashlib
from app.storage import append_event
from dotenv import load_dotenv

load_dotenv()

num_partitions = int(os.getenv("NUM_PARTITIONS"))

def produce_event(user_id : str, topic : str, event : str):
    digest = hashlib.sha256(user_id.encode()).hexdigest()
    int_digest = int(digest, 16)
    partition_id = ((int_digest) % num_partitions) # keeping no. of partitions same under each topic
    # print(hash(user_id))
    append_event(topic, partition_id, event)