## README 😎

### This is a Kafka-inspired, file-backed event streaming system built from first principles, focusing on durable logs, crash-safe consumption, and persistent consumer offsets.

### Concurrency
- Used OS-level file locks -> LOCK_EX, LOCK_UN, to prevent concurrent write corruption
- Ensures only one producer writes to a topic at a time

### Crash-safe append-only logging

#### (Note : Demonstrated crash in this project using os._exit(1))

- If crash occurs mid-write, event will be incompletely written.
- To detect incomplete events, length-prefixed events are written.
- While consuming, if the data read is not of the size which prefixes it, it would imply that event is incomplete, and reading will stop then and there.
- Thus, only valid (complete) events are read and invalid ones are ignored.

### Features of project:

- Crash-safe reads (incomplete events detected)
- Independent consumers
- Offset tracking
- Correct resumption of reads starts if there is a crash mid-consumption

### Divided topics into partitions which are now read by consumer groups

- Partitions are created based on hash (modulo operation : user_id% num_partitions)
- Max. no. of partitions are same for every topic
- Partitions are distributed amongst consumers (there can be a max of three consumers as of now). All partitions are divided amongst three consumers.


### How to run?

1. First create a virtual environment and install requirements.txt by:

    ```bash
    pip install -r requirements.txt
    ```

2. Next, navigate to the root directory of the project (Kafka), and run this:

    ```bash
    uvicorn app.main:app --reload
    ```

3. Browse :  ```http://127.0.0.1:8000```

4. The Fastapi user interface is provided at:

    ```http://127.0.0.1:8000/docs```

    You can explore all the APIs of the Kafka app here one by one...

**Let us dive deep into each endpoint :**

1. ```/``` : This is the root endpoint, which shows a simple greeting message.

2. ```/produce``` : This endpoint allows the user to produce (or add) a new event. It asks for the user id (any integer), the topic name (any name of your choice) and th event name (any name of your choice).

3. ```/consume``` : This endpoint is used when the user wants to consume (or read) the events under a particular topic.
The events of a topic can be read by multiple groups of consumers.
For a group of consumers reading a topic, a particular partition of that topic can b assigned to atmost ONE consumer of the group. This means, no two consumers of "a" group will be reading on the same partition file of a topic.

    Multiple consumer groups can be performing reads on the partition files of a topic at their own speeds.

    They read when they are told to read and each consumer of a group maintains an offset (line number {or event no.}, starting from 0) till where it has read.
    So that next time, it starts its read after the offset.

    This endpoint requires a topic name (should be present as one of the topics in the topic directory...if you want a topic of your own, you need to first create it in the **/produce** endpoint), and a group name (any name of your choice, or one of those present in the **topic_name** directory inside **offsets** directory).

4. ```/fun``` : This is just for fun...You just have to execute it and you get a random name for yourself :)

**Hope you enjoy exploring the Kafka lite !! ☺️**