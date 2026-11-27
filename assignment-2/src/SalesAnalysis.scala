import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SalesAnalysis {
  private val required = Seq(
    "transaction_id", "product_name", "category", "store_location",
    "quantity_sold", "unit_price", "transaction_date"
  )

  private def money(name: String): Column =
    when(col(name).rlike("^[+]?[0-9]+([.][0-9]{1,2})?$"), expr(s"try_cast($name AS DECIMAL(14,2))"))

  private def display(title: String, rows: DataFrame): Unit = {
    println(s"\n$title")
    rows.show(100, truncate = false)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("BDA Assignment 2 - Sales DataFrames").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    try {
      val input = spark.read.option("header", "true").csv(args(0))
      val missing = required.filterNot(input.columns.contains(_))
      require(missing.isEmpty, s"Missing CSV columns: ${missing.mkString(", ")}")
      val hasCost = input.columns.contains("unit_cost")
      val fields = if (hasCost) required :+ "unit_cost" else required
      println("CSV schema as loaded:")
      input.printSchema()

      val raw = input.select(fields.map(name => trim(col(name)).as(name)): _*)
      val names = required.take(4)
      val date = when(col("transaction_date").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
        expr("try_cast(transaction_date AS DATE)"))
      val cost = if (hasCost) money("unit_cost") else lit(null).cast("decimal(14,2)")
      val typed = raw.withColumn("quantity", expr("try_cast(quantity_sold AS INT)"))
        .withColumn("price", money("unit_price")).withColumn("sales_date", date).withColumn("cost", cost)

      val checks = names.map(name => when(col(name).isNull || col(name) === "", lit(name))) ++ Seq(
        when(col("quantity").isNull || col("quantity") <= 0, lit("quantity_sold")),
        when(col("price").isNull || col("price") <= 0, lit("unit_price")),
        when(col("sales_date").isNull, lit("transaction_date"))
      )
      val flagged = typed.withColumn("invalid_columns", concat_ws(", ", checks: _*))
      val salesFields = names.map(col) ++ Seq(col("quantity"), col("price"), col("sales_date"),
        col("cost"), (col("quantity") * col("price")).as("total_sales"))
      val sales = flagged.filter(col("invalid_columns") === "").select(salesFields: _*)

      println("Typed sales schema:")
      sales.printSchema()
      display("Missing or invalid records", flagged.filter(col("invalid_columns") =!= "")
        .select((required :+ "invalid_columns").map(col): _*).orderBy("transaction_id"))
      display("Transaction totals (INR)", sales.drop("cost").orderBy("transaction_id"))

      // This DataFrame describes a computation as named columns and SQL expressions.
      // Catalyst optimizes the plan lazily. show is the action that runs it.
      val job = "category-revenue"
      sc.setJobGroup(job, "Revenue by product category")
      val category = sales.groupBy("category").agg(sum("total_sales").as("revenue"))
        .orderBy(desc("revenue"), col("category"))
      println(s"Category job count before action: ${sc.statusTracker.getJobIdsForGroup(job).length}")
      println("Catalyst can prune unused columns and choose a physical plan from the schema and expressions.")
      println("Partial aggregation reduces shuffle data before the Exchange repartitions by category.")
      println("Final aggregation merges the partial results. AQE can coalesce shuffle partitions.")
      category.explain(extended = true)
      println(s"Category job count after explain: ${sc.statusTracker.getJobIdsForGroup(job).length}")
      display("Revenue by category (INR)", category)
      println(s"Category job count after show: ${sc.statusTracker.getJobIdsForGroup(job).length}")
      sc.clearJobGroup()

      display("Top five products by units sold", sales.groupBy("product_name")
        .agg(sum("quantity").as("units_sold"), sum("total_sales").as("revenue"))
        .orderBy(desc("units_sold"), desc("revenue"), col("product_name")).limit(5))
      display("Revenue by store location (INR)", sales.groupBy("store_location")
        .agg(sum("total_sales").as("revenue")).orderBy(desc("revenue"), col("store_location")))
      display("Transactions strictly above INR 10,000", sales.filter(col("total_sales") > 10000)
        .drop("cost").orderBy(desc("total_sales"), col("transaction_id")))

      if (hasCost) {
        display("Valid sales with missing or invalid unit costs", sales.filter(col("cost").isNull)
          .select("transaction_id", "product_name").orderBy("transaction_id"))
        val profit = sales.groupBy("product_name").agg(
          count(lit(1)).as("transactions"), count("cost").as("costed_transactions"),
          sum(col("quantity") * (col("price") - col("cost"))).as("gross_profit"))
        display("Profitable products by gross profit (INR, complete costs only)", profit
          .filter(col("transactions") === col("costed_transactions") && col("gross_profit") > 0)
          .select("product_name", "gross_profit").orderBy(desc("gross_profit"), col("product_name")))
      }

      // The same validated rows become an RDD of Scala key-value pairs. The map
      // function and reduceByKey explicitly define the work and shuffle in its lineage.
      val dataframeTotals = category.collect().map(row => row.getString(0) -> BigDecimal(row.getDecimal(1))).toMap
      val rddRevenue = sales.select("category", "total_sales").rdd
        .map(row => row.getString(0) -> BigDecimal(row.getDecimal(1))).reduceByKey(_ + _)
      println(s"\nRDD lineage for category revenue:\n${rddRevenue.toDebugString}")
      val rddTotals = rddRevenue.collect().toMap
      rddTotals.toSeq.sortBy(_._1).foreach { case (name, amount) => println(f"  $name: $amount%.2f INR") }
      require(rddTotals == dataframeTotals, "RDD and DataFrame category revenue differ.")
      println("RDD and DataFrame category revenue match.")
      println("RDD tasks follow object transformations and a reduction across partitions.")
      println("The DataFrame optimizer sees the typed columns and aggregation expressions.")
      println("On a cluster, the driver schedules stage tasks and executors fetch shuffle blocks.")
      println(s"Spark UI: ${sc.uiWebUrl.getOrElse("disabled")}")
      println("Open Jobs, select Revenue by product category, and expand DAG Visualization.")
      println("Open the associated SQL execution to inspect its physical plan and runtime metrics.")
      args.lift(1).map(_.toLong).filter(_ > 0).foreach(seconds => Thread.sleep(seconds * 1000))
    } finally spark.stop()
  }
}
