### Concurrency
- Used OS-level file locks -> LOCK_EX, LOCK_UN, to prevent concurrent write corruption
- Ensures only one producer writes to a topic at a time
