# Student academic performance with Spark RDDs

The [synthetic student CSV](data/students.csv) has 12 records from four departments. The Scala application creates `Student` objects with an ID, name, department, semester, and marks, then parallelizes the collection into four partitions.

```bash
python assignment-1/run.py --ui-wait 60
```

The [recorded output](evidence/output.txt) displays all records and the seven marks strictly above 75. Each department has three records. The mean is 80.17, and Nikhil (S010) has the highest score of 96.

## Execution

`map`, `filter`, and `reduceByKey` build new RDDs. Before calling `collect` on the department aggregation, the job group has zero jobs. After the action it has one. The other actions, including `stats` and `max`, likewise trigger execution and return results to the driver.

The Scala program explains task distribution alongside the department aggregation. When `collect` triggers the job, Spark runs four tasks to map and partially combine counts, then shuffles those counts by department. Four more tasks merge the partial counts in the output partitions. The driver receives the final counts. The [Spark DAG](evidence/spark-dag.png) shows this stage boundary.

A separate partition inspection job reports the executor ID and record count for each input partition. In the recorded `local[4]` run, all four partitions processed three records on executor `driver`, which runs tasks on local threads. In cluster mode, the cluster manager allocates executors on worker nodes. The driver schedules tasks on those executors, and the executors fetch shuffle blocks needed by their partitions. [Apache Spark cluster overview](https://spark.apache.org/docs/3.5.8/cluster-overview.html)

See the [completed jobs](evidence/spark-jobs.png) for the actions run during the lab.
