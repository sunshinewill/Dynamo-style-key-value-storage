# Dynamo-style-key-value-storage
Implementing a simplified version of Dynamo

There are three main pieces I have implemented: 1) Partitioning, 2) Replication, and 3) Failure handling.
The main goal is to provide both availability and linearizability at the same time. The read operation always return the most recent value. the partitioning and replication is implemented exactly the way Dynamo does.
