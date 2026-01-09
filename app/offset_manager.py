# What does this file do?
# reads offsets from disk
# writes offsets to disk
# creates offset file if not present
import os

DATA_DIR = "data/offsets"

def get_offset(topic : str, consumer_service : str):
    file_path = os.path.join(DATA_DIR, f'{topic}_{consumer_service}.offset')

    if not os.path.isfile(file_path):
        open(file_path, "x")
        
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
        
def commit_offset(topic : str, consumer_service : str, offset : int):
    off = get_offset(topic, consumer_service)

    file_path = os.path.join(DATA_DIR, f'{topic}_{consumer_service}.offset')

    with open(file_path, "w") as f:
        f.write(str(offset))
        f.flush() # move from memory to OS buffer
        os.fsync(f.fileno()) # write from buffer to disk


# x = get_offset("orders", "email")
# print(x)

# commit_offset("orders", "email", 4)

# x = get_offset("orders", "email")
# print(x)
