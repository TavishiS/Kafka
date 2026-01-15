from app.offset_manager import get_offset, commit_offset
from app.storage import read_from_offset # path bt
from app.group_manager import split_partitions
import os

# eg.: consumer group = email will always read from partition_0 => group_name = email, consumer_id = 0
def consume(topic : str, group_name : str):
    assignments = split_partitions(topic, group_name)
    print("\nAssignment of partitions to consumers is as follows:")
    print(assignments)
    # print("\n")

    if assignments == [[], [], []]:
        print("The given topic does not exist!")

    consumedAllData = [False]*len(assignments)
    i=0
    for consumer in assignments:
        for partition_id in consumer:
            off = get_offset(topic, partition_id, group_name)

            # print(off)

            if off == -2:
                print("The given partition file does not exist!")

            else:
                consumption = read_from_offset(topic, partition_id, off)
                if len(consumption) == 0:
                    consumedAllData[i] = True
                else:
                    for (ind, event) in consumption:
                    # if ind == 4:
                    #     os._exit(1)  --> crash demonstration (if uncommented, crash will occur here, but still, correct consumption will take place)
                        commit_offset(topic, group_name, partition_id, ind)
                        print(f'{event} consumed')
        i+=1
    flag = True
    for boolVal in consumedAllData:
        flag = flag and boolVal

    # for boolVal in consumedAllData:
    #     print(boolVal)
    # print(flag)
    if flag:
        print("Already consumed all data!")

if __name__ == "__main__":
    print("\nTopic : orders, Consumer : email\n")
    consume("orders", "email") # topic, group_name, consumer_id
    print("\nTopic : orders, Consumer : noti\n")
    consume("orders", "noti")
    print("\nTopic : payments, Consumer : email\n")
    consume("payments", "email")
    print("\nTopic : payments, Consumer : noti\n")
    consume("payments", "noti")
    print("\nTopic : cashbacks, Consumer : email")
    consume("cashbacks", "email")
    print("\nTopic : discount, Consumer : email")
    consume("discount", "email")