# What does this file do?
# reads offsets from disk
# writes offsets to disk
# creates offset file if not present
import os
from dotenv import load_dotenv

load_dotenv()

DATA_DIR_OFFSETS = os.getenv("DATA_DIR_OFFSETS")
DATA_DIR_TOPICS = os.getenv("DATA_DIR_TOPICS")

def get_offset(topic : str, partition_id : int, group_name : str):
    new_dir = os.path.join(DATA_DIR_OFFSETS, topic)
    new_dir=os.path.join(new_dir, group_name)
    os.makedirs(new_dir, exist_ok=True)
    file_path = os.path.join(new_dir, f'consumer_{partition_id}.offset')

    if not os.path.isfile(file_path):
        temp_dir = os.path.join(DATA_DIR_TOPICS, topic)
        if os.path.isfile(os.path.join(temp_dir, f'partition_{partition_id}.log')):
            open(file_path, "x") # create offset file if does not exist and partition log file exists
        else:
            # print("The given partition file does not exist!")
            return -2
        
    with open(file_path, "r") as f:
        offset = f.readline()
        if not offset:
            return -1
        else:
            try:
                offset = int(offset)
            except:
                offset = -1
            return offset
        
def commit_offset(topic : str, group_name : str, consumer_id : int, offset : int):
    off = get_offset(topic, consumer_id, group_name)
    
    if off == -2:
        print("The given partition file does not exist!")

    else:
        new_dir = os.path.join(DATA_DIR_OFFSETS, topic)
        new_dir=os.path.join(new_dir, group_name)
        os.makedirs(new_dir, exist_ok=True)
        file_path = os.path.join(new_dir, f'consumer_{consumer_id}.offset')

        with open(file_path, "w") as f:
            f.write(str(offset))
            f.flush() # move from memory to OS buffer
            os.fsync(f.fileno()) # write from buffer to disk


# x = get_offset("orders", "email")
# print(x)

# commit_offset("orders", "email", 4)

# x = get_offset("orders", "email")
# print(x)
