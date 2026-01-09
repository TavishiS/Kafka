# to run this file from Kafka directory, use command : python3 app/storage.py

import os
import fcntl
from dotenv import load_dotenv
from offset_manager import commit_offset

load_dotenv()

DATA_DIR = os.getenv("DATA_DIR_TOPICS")

def append_event(topic : str, event : str):
    os.makedirs(DATA_DIR, exist_ok=True)
    file_path = os.path.join(DATA_DIR, f"{topic}.log")

    encoded = event.encode() # encoded = b'{event}'
    size = len(encoded)
    
    with open(file_path, "ab") as f: # relative paths are resolved according to the current working directory...this will work if we run from directory Kafka...
        fcntl.flock(f, fcntl.LOCK_EX)

        # writing length, then event, then newline -> all encoded

        f.write((str(size)+"\n").encode())
        # f.flush()
        # print("Simulating crash")    
        # os._exit(1)
        f.write(encoded)
        f.write(b"\n")

        f.flush() # to move from memory to OS buffer
        os.fsync(f) # to move from OS buffer to disk

        fcntl.flock(f, fcntl.LOCK_UN)


def read_events(topic : str):
    file_path = os.path.join(DATA_DIR, f'{topic}.log')
    
    events = []

    with open(file_path, "rb") as f:
        ind = 0
        while True:
            size_line = f.readline()

            if not size_line:
                break

            size = int(size_line.strip())

            data = f.read(size)

            if len(data) < size:
                break

            f.read(1)
            data = data.decode()
            events.append((ind, data.strip()))
            ind+=1

    return events

def read_from_offset(topic : str, offset : int):
    all_events = read_events(topic)

    consumption = []

    for (ind, event) in all_events:
        if ind > offset:
            consumption.append((ind, event))


    return consumption

# The yield keyword turns a function into a function generator.
# The function generator returns an iterator.
# The code inside the function is not executed when they are first called, but are divided into steps, one step for each yield, and each step is only executed when iterated upon.

# Test runs

if __name__ == "__main__":
    events = read_events("orders")

    for event in events:
        print(event)

    consumption = read_from_offset("orders", 1)

    for event in consumption:
        print(event)