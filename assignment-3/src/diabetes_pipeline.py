import argparse
import csv
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F

from prepare_dataset import DEFAULT_OUTPUT, FEATURES, LABEL

HERE = Path(__file__).resolve().parents[1]
SEED = 24012
BOUNDS = {name: (0, 1) for name in FEATURES}
BOUNDS.update(BMI=(1, 100), GenHlth=(1, 5), Age=(1, 13))
METRICS = ("accuracy", "precision", "recall", "f1", "tn", "fp", "fn", "tp")


def clean_features(raw):
    for field, (low, high) in BOUNDS.items():
        numeric = F.expr(f"try_cast(`{field}` AS DOUBLE)")
        raw = raw.withColumn(
            field, F.when(numeric.between(low, high) & (numeric == F.floor(numeric)), numeric)
        )
    return raw


def make_pipeline(classifier):
    imputed = [f"{field}_imputed" for field in FEATURES]
    return Pipeline(stages=[
        Imputer(strategy="median", inputCols=list(FEATURES), outputCols=imputed),
        VectorAssembler(inputCols=imputed, outputCol="unscaled"),
        StandardScaler(inputCol="unscaled", outputCol="features", withStd=True, withMean=False),
        classifier,
    ])


def safe_ratio(numerator, denominator):
    return numerator / denominator if denominator else 0.0


def evaluate(predictions):
    cells = {(int(row.label), int(row.prediction)): row["count"]
             for row in predictions.groupBy("label", "prediction").count().collect()}
    tn, fp, fn, tp = (cells.get(pair, 0) for pair in ((0, 0), (0, 1), (1, 0), (1, 1)))
    precision, recall = safe_ratio(tp, tp + fp), safe_ratio(tp, tp + fn)
    return dict(zip(METRICS, (
        safe_ratio(tn + tp, tn + fp + fn + tp), precision, recall,
        safe_ratio(2 * precision * recall, precision + recall), tn, fp, fn, tp,
    )))


def write_csv(path, fields, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main(args):
    args.output.mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder.appName("BDA Assignment 3 - Diabetes MLlib").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    try:
        raw = spark.read.option("header", True).csv(str(args.data.resolve()))
        required = {*FEATURES, LABEL}
        if not required.issubset(raw.columns):
            raise ValueError(f"Missing columns: {sorted(required - set(raw.columns))}")
        label_value = F.expr(f"try_cast(`{LABEL}` AS DOUBLE)")
        clean = clean_features(raw.select(LABEL, *FEATURES)).withColumn(
            "label", F.when(label_value.isin(0.0, 1.0), label_value)
        ).drop(LABEL)
        missing = clean.select(*(F.count(F.when(F.col(c).isNull(), 1)).alias(c)
                                 for c in (*FEATURES, "label"))).first()
        print("Missing or invalid counts:", missing.asDict(), flush=True)

        # Keep each identical feature vector entirely within one split.
        bucket = F.pmod(F.xxhash64(F.lit(SEED), *FEATURES), F.lit(100))
        split_data = clean.filter(F.col("label").isNotNull()).withColumn("bucket", bucket).cache()
        train = split_data.filter(F.col("bucket") < 65)
        validation = split_data.filter(F.col("bucket").between(65, 79))
        test = split_data.filter(F.col("bucket") >= 80)
        for name, split in (("Train", train), ("Validation", validation), ("Test", test)):
            counts = {int(row.label): row["count"] for row in split.groupBy("label").count().collect()}
            if set(counts) != {0, 1}:
                raise ValueError(f"{name} split must contain both classes: {counts}")
            print(f"{name} class counts: {counts}", flush=True)

        classifiers = {
            "Logistic Regression": LogisticRegression(maxIter=35, regParam=0.01),
            "Decision Tree": DecisionTreeClassifier(maxDepth=6, seed=SEED),
            "Random Forest": RandomForestClassifier(numTrees=25, maxDepth=7, seed=SEED),
        }
        models = {}
        validation_results = {}
        for name, classifier in classifiers.items():
            model = make_pipeline(classifier).fit(train)
            models[name] = model
            validation_results[name] = evaluate(model.transform(validation))
            print(f"{name} validation F1: {validation_results[name]['f1']:.4f}", flush=True)
        best = max(classifiers, key=lambda name: validation_results[name]["f1"])
        print(f"Selected by validation F1: {best}", flush=True)

        scores = []
        for name, model in models.items():
            measures = evaluate(model.transform(test))
            print(f"{name} test metrics: {measures}", flush=True)
            scores.append({"model": name, "selected": name == best,
                           "validation_f1": f"{validation_results[name]['f1']:.6f}",
                           **{key: f"{val:.6f}" if isinstance(val, float) else val
                              for key, val in measures.items()}})
        write_csv(args.output / "metrics.csv", ("model", "selected", "validation_f1", *METRICS), scores)

        new_rows = spark.read.option("header", True).csv(str(args.patients.resolve()))
        predictions = models[best].transform(clean_features(new_rows)).select(
            "case_id", F.col("prediction").cast("int").alias("predicted_class"),
            F.round(vector_to_array("probability")[1], 6).alias("positive_class_score"),
        ).orderBy("case_id")
        results = [row.asDict() for row in predictions.collect()]
        for result in results:
            print(result, flush=True)
        write_csv(args.output / "predictions.csv",
                  ("case_id", "predicted_class", "positive_class_score"), results)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare Spark MLlib diabetes classifiers.")
    parser.add_argument("--data", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--patients", type=Path, default=HERE / "data" / "new_patients.csv")
    parser.add_argument("--output", type=Path, default=HERE / "evidence")
    main(parser.parse_args())
