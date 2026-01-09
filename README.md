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
