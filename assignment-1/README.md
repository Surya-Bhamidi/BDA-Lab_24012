# Student academic performance with Spark RDDs

The [synthetic student CSV](data/students.csv) has 12 records from four departments. The Scala application creates `Student` objects with an ID, name, department, semester, and marks, then parallelizes the collection into four partitions.

```bash
python assignment-1/run.py --ui-wait 60
```

The [recorded output](evidence/output.txt) displays all records and the seven marks strictly above 75. Each department has three records. The mean is 80.17, and Nikhil (S010) has the highest score of 96.

## Execution

`map`, `filter`, and `reduceByKey` build new RDDs. Before calling `collect` on the department aggregation, the job group has zero jobs. After the action it has one. The other actions, including `stats` and `max`, likewise trigger execution and return results to the driver.

The [Spark DAG](evidence/spark-dag.png) and the printed `toDebugString` show the parallelized collection, the mapping into key-value pairs, and the shuffle for `reduceByKey`. The driver schedules stages and assigns tasks for each partition. Tasks apply narrow operations within a partition, then the shuffle sends partial counts to the partitions responsible for each key. In this local run the four partitions have three records each and run on the driver's Spark threads. In cluster mode executors on worker nodes run those same partition tasks and fetch shuffle data over the network.

See the [completed jobs](evidence/spark-jobs.png) for the actions run during the lab.
