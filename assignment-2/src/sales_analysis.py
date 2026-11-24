import argparse
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

DEFAULT_DATA = Path(__file__).resolve().parents[1] / "data" / "sales.csv"
COLUMNS = (
    "transaction_id", "product_name", "category", "store_location",
    "quantity_sold", "unit_price", "transaction_date",
)


def display(title, rows):
    print(f"\n{title}", flush=True)
    rows.show(100, truncate=False)


def main(path, ui_wait):
    spark = SparkSession.builder.appName("BDA Assignment 2 - Sales DataFrames").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    try:
        input_data = spark.read.option("header", True).csv(str(path))
        if tuple(input_data.columns) != COLUMNS:
            raise ValueError(f"Unexpected CSV columns: {input_data.columns}")

        raw = input_data.select(*(F.trim(F.col(c)).alias(c) for c in COLUMNS))
        names = COLUMNS[:4]
        typed = raw.select(
            "*",
            F.expr("try_cast(quantity_sold AS INT)").alias("quantity"),
            F.when(
                F.col("unit_price").rlike(r"^[+]?[0-9]+([.][0-9]{1,2})?$"),
                F.expr("try_cast(unit_price AS DECIMAL(14,2))"),
            ).alias("price"),
            F.when(
                F.col("transaction_date").rlike(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
                F.expr("try_cast(transaction_date AS DATE)"),
            ).alias("sales_date"),
        )
        checks = {
            **{c: F.col(c).isNull() | (F.col(c) == "") for c in names},
            "quantity_sold": F.col("quantity").isNull() | (F.col("quantity") <= 0),
            "unit_price": F.col("price").isNull() | (F.col("price") <= 0),
            "transaction_date": F.col("sales_date").isNull(),
        }
        flagged = typed.withColumn("invalid_columns", F.concat_ws(
            ", ", *(F.when(test, F.lit(name)) for name, test in checks.items())
        ))
        sales = flagged.filter(F.col("invalid_columns") == "").select(
            *names, "quantity", "price", "sales_date",
            (F.col("quantity") * F.col("price")).alias("total_sales"),
        )
        print("Typed sales schema")
        sales.printSchema()
        display("Missing or invalid records", flagged.filter(F.col("invalid_columns") != "")
                .select(*COLUMNS, "invalid_columns").orderBy("transaction_id"))
        display("Transaction totals (INR)", sales.orderBy("transaction_id"))

        sc.setJobGroup("category-revenue", "Revenue by product category")
        category_revenue = sales.groupBy("category").agg(F.sum("total_sales").alias("revenue"))
        category_revenue = category_revenue.orderBy(F.desc("revenue"), "category")
        print(f"Category job count before action: {len(sc.statusTracker().getJobIdsForGroup('category-revenue'))}")
        category_revenue.explain(mode="extended")
        display("Revenue by category (INR)", category_revenue)
        print(f"Category job count after show: {len(sc.statusTracker().getJobIdsForGroup('category-revenue'))}")
        for key in ('spark.jobGroup.id', 'spark.job.description', 'spark.job.interruptOnCancel'):
            sc.setLocalProperty(key, None)

        best_sellers = sales.groupBy("product_name").agg(
            F.sum("quantity").alias("units_sold"), F.sum("total_sales").alias("revenue")
        ).orderBy(F.desc("units_sold"), F.desc("revenue"), "product_name")
        display("Top five products by units sold", best_sellers.limit(5))
        display("Revenue by store location (INR)", sales.groupBy("store_location").agg(
            F.sum("total_sales").alias("revenue")
        ).orderBy(F.desc("revenue"), "store_location"))
        display("Transactions strictly above INR 10,000", sales.filter(
            F.col("total_sales") > F.lit(10000)
        ).orderBy(F.desc("total_sales"), "transaction_id"))
        print(f"Spark UI: {sc.uiWebUrl}", flush=True)
        time.sleep(ui_wait)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyse store transactions using Spark SQL.")
    parser.add_argument("--data", default=DEFAULT_DATA, type=Path)
    parser.add_argument("--ui-wait", type=int, default=0, help="Seconds to leave the Spark UI open")
    args = parser.parse_args()
    main(args.data.resolve(), args.ui_wait)
