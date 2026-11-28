import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.Locale

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.feature.{Imputer, StandardScaler, VectorAssembler}
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DiabetesPipeline {
  private val features = PrepareDataset.Features
  private val target = PrepareDataset.Label
  private val seed = 24012L
  private val bounds = features.map(_ -> (0, 1)).toMap ++ Map(
    "BMI" -> (1, 100), "GenHlth" -> (1, 5), "Age" -> (1, 13)
  )

  private case class Metrics(tn: Long, fp: Long, fn: Long, tp: Long) {
    val total: Long = tn + fp + fn + tp
    val accuracy: Double = ratio(tn + tp, total)
    val precision: Double = ratio(tp, tp + fp)
    val recall: Double = ratio(tp, tp + fn)
    val f1: Double = ratio(2 * tp, 2 * tp + fp + fn)
  }

  private case class Score(name: String, validationF1: Double, test: Metrics)

  private def ratio(numerator: Long, denominator: Long): Double =
    if (denominator == 0) 0.0 else numerator.toDouble / denominator

  private def decimal(value: Double, places: Int): String =
    String.format(Locale.ROOT, s"%.${places}f", Double.box(value))

  private def readCsv(spark: SparkSession, path: String, required: Seq[String]): DataFrame = {
    val input = spark.read.option("header", "true").csv(path)
    val missing = required.filterNot(input.columns.contains(_))
    require(missing.isEmpty, s"Missing CSV columns: ${missing.mkString(", ")}")
    input
  }

  private def cleanFeatures(input: DataFrame): DataFrame =
    features.foldLeft(input) { (rows, name) =>
      val (low, high) = bounds(name)
      val numeric = expr(s"try_cast($name AS DOUBLE)")
      rows.withColumn(name, when(numeric.between(low, high) && numeric === floor(numeric), numeric))
    }

  private def cleanTrainingData(input: DataFrame): DataFrame = {
    val fields = features :+ target
    val originals = fields.map(name => col(name).as(s"raw_$name"))
    val label = expr(s"try_cast($target AS DOUBLE)")
    val clean = cleanFeatures(input.select((fields.map(col) ++ originals): _*))
      .withColumn("label", when(label.isin(0.0, 1.0), label))
    val checks = fields.flatMap { name =>
      val raw = col(s"raw_$name")
      val missing = raw.isNull || trim(raw) === ""
      val parsed = col(if (name == target) "label" else name)
      Seq(
        count(when(missing, 1)).as(s"${name}_missing"),
        count(when(!missing && parsed.isNull, 1)).as(s"${name}_invalid")
      )
    }
    val quality = clean.agg(checks.head, checks.tail: _*).first()
    println("Data quality (blank or null, invalid):")
    fields.foreach { name =>
      println(s"  $name: ${quality.getAs[Long](s"${name}_missing")}, ${quality.getAs[Long](s"${name}_invalid")}")
    }
    println("Rows with missing or invalid labels are excluded. Fitted medians fill missing features.")
    clean.select((features.map(col) :+ col("label")): _*).filter(col("label").isNotNull)
  }

  private def pipeline(classifier: PipelineStage): Pipeline = {
    // Only the training split fits the medians, scaling statistics, and model.
    // The fitted stages transform validation, test, and new records identically.
    val imputed = features.map(_ + "_imputed").toArray
    new Pipeline().setStages(Array[PipelineStage](
      new Imputer().setStrategy("median").setInputCols(features.toArray).setOutputCols(imputed),
      new VectorAssembler().setInputCols(imputed).setOutputCol("unscaled"),
      new StandardScaler().setInputCol("unscaled").setOutputCol("features").setWithStd(true).setWithMean(false),
      classifier
    ))
  }

  private def evaluate(predictions: DataFrame): Metrics = {
    // Spark aggregates confusion counts on executors. Only four cells reach the driver.
    val cells = predictions.groupBy("label", "prediction").count().collect().map { row =>
      (row.getDouble(0).toInt, row.getDouble(1).toInt) -> row.getLong(2)
    }.toMap
    def cell(label: Int, prediction: Int): Long = cells.getOrElse((label, prediction), 0L)
    Metrics(cell(0, 0), cell(0, 1), cell(1, 0), cell(1, 1))
  }

  private def csv(path: Path, header: Seq[String], rows: Seq[Seq[String]]): Unit = {
    def escape(value: String): String =
      if (value.exists(c => c == ',' || c == '"' || c == '\r' || c == '\n'))
        "\"" + value.replace("\"", "\"\"") + "\""
      else value
    val lines = (header +: rows).map(_.map(escape).mkString(","))
    Files.write(path, (lines.mkString("\n") + "\n").getBytes(UTF_8))
  }

  private def writeResults(output: Path, scores: Seq[Score], selected: Score): Unit = {
    def values(score: Score, places: Int): Seq[String] =
      Seq(score.validationF1, score.test.accuracy, score.test.precision, score.test.recall, score.test.f1)
        .map(decimal(_, places))
    csv(output.resolve("metrics.csv"),
      Seq("model", "selected", "validation_f1", "accuracy", "precision", "recall", "f1", "tn", "fp", "fn", "tp"),
      scores.map { score =>
        val test = score.test
        Seq(score.name, (if (score.name == selected.name) "True" else "False")) ++ values(score, 6) ++
          Seq(test.tn, test.fp, test.fn, test.tp).map(_.toString)
      })

    val bestF1 = scores.maxBy(_.test.f1)
    val bestPrecision = scores.maxBy(_.test.precision)
    val testSize = String.format(Locale.ROOT, "%,d", Long.box(selected.test.total))
    val table = scores.map(score => (score.name +: values(score, 4)).mkString("| ", " | ", " |"))
    val report = (Seq(
      "# Assignment 3 model comparison",
      "",
      s"Accuracy, precision, recall, and F1 were measured on the same $testSize held-out rows.",
      "Precision, recall, and F1 refer to label 1 (prediabetes or diabetes).",
      "",
      "| Model | Validation F1 | Accuracy | Precision | Recall | Test F1 |",
      "| --- | ---: | ---: | ---: | ---: | ---: |"
    ) ++ table ++ Seq(
      "",
      "## Interpretation",
      "",
      s"${selected.name} was selected for the fictional patient predictions because it had the highest " +
        s"validation F1 (${decimal(selected.validationF1, 4)}). F1 is the harmonic mean of precision and recall.",
      "",
      s"${bestF1.name} had the highest test F1 (${decimal(bestF1.test.f1, 4)}). " +
        s"The selected model detected ${decimal(selected.test.recall * 100, 1)}% of actual positives, " +
        s"and ${decimal(selected.test.precision * 100, 1)}% of its positive predictions were correct. " +
        s"${bestPrecision.name} had the highest test precision (${decimal(bestPrecision.test.precision * 100, 1)}%).",
      "",
      "These scores describe performance on this balanced survey dataset.",
      "",
      "[Metrics and confusion counts](metrics.csv) | [Fictional patient predictions](predictions.csv)"
    )).mkString("\n") + "\n"
    Files.write(output.resolve("model_comparison.md"), report.getBytes(UTF_8))
    println("\n" + report)
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 3, "Usage: DiabetesPipeline <training.csv> <new-patients.csv> <output-dir>")
    val output = Paths.get(args(2))
    Files.createDirectories(output)
    val spark = SparkSession.builder().appName("BDA Assignment 3 - Scala Spark MLlib").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      println(s"Input features: ${features.mkString(", ")}")
      println(s"$target maps to label (0 = negative, 1 = prediabetes or diabetes)")
      val clean = cleanTrainingData(readCsv(spark, args(0), features :+ target))

      // Hash cleaned features so identical profiles stay in one split.
      val bucket = pmod(xxhash64((lit(seed.toInt) +: features.map(col)): _*), lit(100))
      val partitions = spark.conf.get("spark.sql.shuffle.partitions").toInt
      val data = clean.withColumn("bucket", bucket).repartition(partitions, col("bucket")).cache()
      val train = data.filter(col("bucket") < 65)
      val validation = data.filter(col("bucket").between(65, 79))
      val test = data.filter(col("bucket") >= 80)
      Seq("Train" -> train, "Validation" -> validation, "Test" -> test).foreach { case (name, rows) =>
        val counts = rows.groupBy("label").count().orderBy("label").collect()
        require(counts.map(_.getDouble(0)).toSeq == Seq(0.0, 1.0), s"$name must contain both classes")
        val summary = counts.map(row => s"${row.getDouble(0).toInt}: ${row.getLong(1)}").mkString(", ")
        println(s"$name class counts: {$summary}")
      }

      // Executors compute partition-local gradients or tree split statistics.
      // Spark aggregates these partial results between iterations.
      println(s"Training rows per partition (${spark.sparkContext.master}):")
      train.groupBy(spark_partition_id().as("partition")).count().orderBy("partition").collect()
        .foreach(row => println(s"  Partition ${row.getInt(0)}: ${row.getLong(1)} rows"))
      println("On a cluster, executors train on partitions and Spark combines partial statistics.")
      println("Partitioned aggregation also computes the evaluation counts.")

      val classifiers = Seq(
        "Logistic Regression" -> new LogisticRegression().setMaxIter(35).setRegParam(0.01),
        "Decision Tree" -> new DecisionTreeClassifier().setMaxDepth(6).setSeed(seed),
        "Random Forest" -> new RandomForestClassifier().setNumTrees(25).setMaxDepth(7).setSeed(seed)
      )
      val fitted = classifiers.map { case (name, classifier) =>
        val stages = pipeline(classifier)
        println(s"$name pipeline: ${stages.getStages.map(_.getClass.getSimpleName).mkString(" -> ")}")
        val model = stages.fit(train)
        val validationF1 = evaluate(model.transform(validation)).f1
        println(s"$name validation F1: ${decimal(validationF1, 4)}")
        (name, model, validationF1)
      }
      // Select on validation before using the held-out test set.
      val (selectedName, selectedModel, _) = fitted.maxBy(_._3)
      println(s"Selected by validation F1: $selectedName")
      val scores = fitted.map { case (name, model, validationF1) =>
        Score(name, validationF1, evaluate(model.transform(test)))
      }
      writeResults(output, scores, scores.find(_.name == selectedName).get)
      println("MLlib test input schema:")
      selectedModel.transform(test).select("features", "label").printSchema()

      val patients = readCsv(spark, args(1), "case_id" +: features)
      val predictions = selectedModel.transform(cleanFeatures(patients)).select(
        col("case_id"), col("prediction").cast("int").as("predicted_class"),
        round(vector_to_array(col("probability")).getItem(1), 6).as("positive_class_score"),
        col("BMI_imputed").as("bmi_used")
      ).orderBy("case_id").collect().toSeq
      val rows = predictions.map(row => Seq(Option(row.getString(0)).getOrElse(""), row.getInt(1).toString,
        row.getDouble(2).toString, row.getDouble(3).toString))
      rows.foreach(row => println(row.mkString(", ")))
      csv(output.resolve("predictions.csv"), Seq("case_id", "predicted_class", "positive_class_score", "bmi_used"), rows)
    } finally spark.stop()
  }
}
