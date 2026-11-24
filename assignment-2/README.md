# Retail sales and business intelligence with Spark DataFrames

The [synthetic sales CSV](data/sales.csv) contains 20 transactions. Four intentionally invalid records test a missing product, negative quantity, nonnumeric price, and impossible date. The application prints the rejected records with their invalid columns. It computes sales for the remaining 16 records using decimal prices and reports amounts in INR. Product rank uses units sold, with revenue and product name breaking ties.

```bash
python assignment-2/src/sales_analysis.py --ui-wait 60
```

The [recorded output](evidence/output.txt) includes the typed schema, invalid rows, transaction amounts, category totals, five best sellers, location totals, and seven transactions strictly above INR 10,000. Electronics generated INR 175,550, and Hyderabad had the highest location revenue at INR 99,670. Notebooks ranked first by volume (80 units). The dataset has no cost column, so the reported amounts are revenue.

## Comparing RDD and DataFrame execution

| Aspect | RDD | DataFrame |
| --- | --- | --- |
| Data representation | Partitioned Scala objects or key-value pairs | Named columns with a schema |
| How operations are expressed | Functions such as `map` and `reduceByKey` | Expressions such as `filter`, `groupBy`, and `sum` |
| Planning | Spark schedules tasks from lineage and shuffles at wide dependencies | Catalyst plans column expressions, then Spark schedules the physical plan |
| Evidence | [Assignment 1 DAG](../assignment-1/evidence/spark-dag.png) | [SQL plan graph](evidence/spark-sql-plan.png) and [job DAG](evidence/spark-dag.png) |

The category query creates no job until `show` runs. The extended plan exposes parsed, analyzed, optimized, and physical plans. The physical plan shows a partial hash aggregation, an exchange by category, and a final aggregation. Sort and display limits can appear in the executed plan, and Adaptive Query Execution can adjust shuffle partitioning. The [Spark Jobs screenshot](evidence/spark-jobs.png) shows the resulting jobs. In cluster mode the driver plans and schedules execution, while executors operate on the partitions and exchange shuffle data as needed.
