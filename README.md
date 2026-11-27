# BDA Lab

The repository contains three independent labs for Big Data Analytics (23AID302).

| Assignment | Implementation | Run evidence |
| --- | --- | --- |
| [1 · Student academic performance](assignment-1/README.md) | Scala and Spark RDDs | [Output](assignment-1/evidence/output.txt) · [RDD DAG](assignment-1/evidence/spark-dag.png) |
| [2 · Sales and business intelligence](assignment-2/README.md) | Scala DataFrames and RDDs | [Output and plans](assignment-2/evidence/output.txt) · [SQL DAG](assignment-2/evidence/spark-sql-plan.png) |
| [3 · Diabetes classification](assignment-3/README.md) | PySpark MLlib | [Metrics](assignment-3/evidence/metrics.csv) · [Predictions](assignment-3/evidence/predictions.csv) |

## Run locally

Install Java 17 and Python 3.10–3.12. Set `JAVA_HOME` and install Spark's Python distribution, which also provides the Scala 2.12 compiler and Spark launcher used in Assignment 1.

```bash
python -m pip install -r requirements.txt
python assignment-1/run.py
python assignment-2/src/sales_analysis.py
python assignment-3/src/diabetes_pipeline.py
```

Spark usually listens at `http://localhost:4040` while a job is running. Pass `--ui-wait 60` to Assignment 1 or 2 to keep it available for inspection after the job finishes. In the Jobs tab, open the named aggregation and expand **DAG Visualization**. For Assignment 2, the SQL tab shows the associated query plan graph. On Windows, set `HADOOP_HOME` to your compatible Hadoop native binaries if Spark reports a native library error. On a remote cluster, submit with the cluster's master and use a dataset path accessible to the appropriate Spark processes.
