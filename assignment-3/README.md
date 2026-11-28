# Diabetes classification with Spark MLlib

The training CSV is an 11-column projection of the balanced 2015 CDC Behavioral Risk Factor Surveillance System dataset. The dataset uses self-reported diabetes categories. The supplied binary target is 0 for the negative survey class and 1 for prediabetes or diabetes. The 70,692 rows and their original labels are preserved. The coursework uses 10 inputs: high blood pressure, high cholesterol, BMI, smoking, stroke, heart disease or attack, physical activity, general health, difficulty walking, and age band.

- [Publisher and CC0 license](https://www.kaggle.com/datasets/alexteboul/diabetes-health-indicators-dataset)
- [Download the upstream ZIP](https://www.kaggle.com/api/v1/datasets/download/alexteboul/diabetes-health-indicators-dataset)
- [UCI data dictionary](https://archive.ics.uci.edu/dataset/891/cdc+diabetes+health+indicators)

## Run the Scala application

[`DiabetesPipeline.scala`](src/DiabetesPipeline.scala) implements the Spark MLlib workflow in Scala 2.12. [`PrepareDataset.scala`](src/PrepareDataset.scala) recreates the training CSV from the publisher's archive.

From the repository root with Java 17 and the bundled Spark tooling:

```bash
python -m pip install -r requirements.txt
python assignment-3/run.py train
python assignment-3/run.py prepare /path/to/upstream.zip --output /path/to/diabetes_brfss_2015.csv
```

The Python launcher compiles and starts Scala programs. For a native Scala setup with sbt, Java 17, and Spark 3.5.5 built for Scala 2.12:

```bash
cd assignment-3
sbt package
spark-submit --master 'local[4]' --driver-memory 2g --conf spark.sql.shuffle.partitions=4 --class DiabetesPipeline target/scala-2.12/diabetes-ml_2.12-1.0.0.jar data/diabetes_brfss_2015.csv data/new_patients.csv evidence
```

## Pipeline and evaluation

Spark reads the CSV as a DataFrame. Values are cast and checked against each input's valid range. Blank, invalid, or out-of-range inputs become null. The program counts missing and invalid values separately for every input and the target, and excludes rows with unusable labels. A deterministic feature hash keeps identical feature vectors in one split to avoid overlap. The actual split has 45,618 training, 10,881 validation, and 14,193 test rows with training spread across four Spark partitions.

Each classifier has a Spark Pipeline with training-set median imputation, feature assembly, standard deviation scaling, and an estimator. The pipeline compares Logistic Regression, Decision Tree, and Random Forest. Model selection uses positive-class F1 on validation data. All three models are then evaluated on the held-out test split. The app prints the pipeline stages, the MLlib `features` vector and numeric `label` schema, and a short interpretation of the model comparison.

## Results

The run prints and saves a [performance comparison table and interpretation](evidence/model_comparison.md). The [metrics CSV](evidence/metrics.csv) includes full-precision scores and confusion counts.

Random Forest scored three [fictional examples](data/new_patients.csv) as classes 0, 1, and 1, including an example with missing BMI. The output records the fitted training median of 29.0 used for that example. See the [prediction scores](evidence/predictions.csv) and [run output](evidence/output.txt). These predictions are classroom demonstrations based on survey labels.

Spark can distribute CSV reading, transformations, model training, and metric aggregation across partitions. Executors compute local statistics such as imputation summaries, gradient contributions, or split histograms, and Spark combines those statistics before the next algorithm step. The run output shows the training row count in each partition. Only summaries and the three fictional predictions are collected to the driver. [Spark MLlib pipeline guide](https://spark.apache.org/docs/3.5.8/ml-pipeline.html)
