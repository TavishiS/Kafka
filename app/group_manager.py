import os
from dotenv import load_dotenv

load_dotenv()

DATA_DIR_TOPICS = os.getenv("DATA_DIR_TOPICS")

def split_partitions(topic : str, group_name : str):
    new_dir = os.path.join(DATA_DIR_TOPICS, topic) # mistake thi pehle
    os.makedirs(new_dir, exist_ok=True)

    num_partitions=0
    partitions = []

    for entity in os.listdir(new_dir):
        if os.path.isfile(os.path.join(new_dir, entity)):
            num_partitions+=1
            partition_id = entity.split('_')[1].split('.')[0]
            partitions.append(partition_id)

    partitions.sort()

    assignments = [[] for _ in range(3)] # size of assignments list = 3 (max consumers)

    i=0
    while(i<num_partitions):
        assignments[i%3].append(partitions[i]) # assuming that no. of consumers should not exceed 3
        i+=1

    if assignments == [[],[],[]]:
        os.rmdir(new_dir)
    return assignments