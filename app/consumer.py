from offset_manager import get_offset, commit_offset
from storage import read_from_offset
import os

def consume(topic : str, consumer_service : str):
    off = get_offset(topic, consumer_service)

    consumption = read_from_offset(topic, off)

    for (ind, event) in consumption:
        # if ind == 4:
        #     os._exit(1)  --> crash demonstration (if uncommented, crash will occur here, but still, correct consumption will take place)
        commit_offset(topic, consumer_service, ind)
        print(f'{event} consumed')

    if consumption == []:
        print("Already consumed all data!")

if __name__ == "__main__":
    print("\nConsumer : email\n")
    consume("orders", "email") # topic, consumer
    print("\nConsumer : payments\n")
    consume("orders", "payments")