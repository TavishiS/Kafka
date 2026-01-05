import fcntl
import os

def append_event(topic : str, event : str):
    with open(f"data/topics/{topic}.log", "a") as f: # relative paths are resolved according to the current working directory...this will work if we run from directory Kafka...
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(event+"\n")
        f.flush() # to move from memory to OS buffer
        # os.fsync(f) # to move from OS buffer to disk
        fcntl.flock(f, fcntl.LOCK_UN)