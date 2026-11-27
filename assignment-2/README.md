# Retail sales and business intelligence with Spark DataFrames

The [synthetic sales CSV](data/sales.csv) contains the seven required fields plus a synthetic `unit_cost`. Four intentionally invalid records test a missing product, negative quantity, nonnumeric price, and impossible date. The app prints the input and typed schemas, identifies the affected fields, and computes reports from the 16 valid transactions. Amounts use decimal arithmetic in INR. Products rank by units sold, then revenue and product name.

```bash
python assignment-2/src/sales_analysis.py --ui-wait 60
```

The [recorded output](evidence/output.txt) includes the typed schema, invalid rows, transaction amounts, category totals, five best sellers, location totals, and seven transactions strictly above INR 10,000. Electronics generated INR 175,550, and Hyderabad had the highest location revenue at INR 99,670. Notebooks ranked first by volume (80 units). Laptops had the most gross profit (INR 14,000), calculated by multiplying quantity by the difference between unit price and unit cost for each transaction, then summing. The profit report includes only products with a valid cost for every included sale. A seven-column input still produces the revenue reports.

The [Scala implementation](src/SalesAnalysis.scala) uses Spark SQL and the Scala RDD API. The Python launcher uses the Scala compiler bundled with Spark.

## Comparing RDD and DataFrame execution

| Aspect | RDD | DataFrame |
| --- | --- | --- |
| Data representation | Partitioned Scala objects or key-value pairs | Named columns with a schema |
| How operations are expressed | Functions such as `map` and `reduceByKey` | Expressions such as `filter`, `groupBy`, and `sum` |
| Planning | Spark schedules tasks from lineage and shuffles at wide dependencies | Catalyst plans column expressions, then Spark schedules the physical plan |
| Evidence | RDD lineage and totals in the [recorded output](evidence/output.txt) | [SQL plan graph](evidence/spark-sql-plan.png) and [job DAG](evidence/spark-dag.png) |

The program converts the same validated rows to an RDD of Scala key-value pairs and repeats the category-revenue calculation with `map` and `reduceByKey` and checks that all totals match the DataFrame result. The DataFrame category query creates no job during plan construction or `explain`. Its `show` action starts execution. The extended plan exposes parsed, analyzed, optimized, and physical plans. The physical plan shows a partial hash aggregation, an exchange by category, and a final aggregation. Sort and display limits can appear in the executed plan, and Adaptive Query Execution can adjust shuffle partitioning. The [Spark Jobs screenshot](evidence/spark-jobs.png) shows the resulting jobs. In cluster mode the driver plans and schedules execution, while executors operate on the partitions and exchange shuffle data as needed. [Spark SQL guide](https://spark.apache.org/docs/3.5.8/sql-programming-guide.html)
