# Diabetes classification with Spark MLlib

The training CSV is an 11-column projection of the balanced 2015 CDC Behavioral Risk Factor Surveillance System dataset. The dataset uses self-reported diabetes categories. The supplied binary target is 0 for the negative survey class and 1 for prediabetes or diabetes. The 70,692 rows and their original labels are preserved. The coursework uses 10 inputs: high blood pressure, high cholesterol, BMI, smoking, stroke, heart disease or attack, physical activity, general health, difficulty walking, and age band.

- [Publisher and CC0 license](https://www.kaggle.com/datasets/alexteboul/diabetes-health-indicators-dataset)
- [Download the upstream ZIP](https://www.kaggle.com/api/v1/datasets/download/alexteboul/diabetes-health-indicators-dataset)
- [UCI data dictionary](https://archive.ics.uci.edu/dataset/891/cdc+diabetes+health+indicators)

```bash
python assignment-3/src/diabetes_pipeline.py
```

Run `python assignment-3/src/prepare_dataset.py /path/to/upstream.zip` to regenerate the training CSV from the publisher's archive.

## Pipeline and evaluation

Spark reads the CSV as a DataFrame. Values are cast and checked against each input's valid range. Blank, invalid, or out-of-range inputs become null. The program counts missing and invalid values separately for every input and the target, and excludes rows with unusable labels. A deterministic feature hash keeps identical feature vectors in one split to avoid overlap. The actual split has 45,618 training, 10,881 validation, and 14,193 test rows with training spread across four Spark partitions.

Each classifier has a Spark Pipeline with training-set median imputation, feature assembly, standard deviation scaling, and an estimator. The pipeline compares Logistic Regression, Decision Tree, and Random Forest. Model selection uses positive-class F1 on validation data. All three models are then evaluated on the held-out test split. The app prints the pipeline stages, the MLlib `features` vector and numeric `label` schema, and a short interpretation of the model comparison.

| Classifier | Accuracy | Precision | Recall | F1 |
| --- | ---: | ---: | ---: | ---: |
| Logistic Regression | 0.7432 | 0.7332 | 0.7625 | 0.7476 |
| Decision Tree | 0.7387 | 0.7135 | 0.7954 | 0.7522 |
| Random Forest | 0.7449 | 0.7188 | 0.8025 | 0.7583 |

Random Forest had the best validation F1 (0.7520) and the highest test F1. Logistic Regression had higher positive-class precision. The [complete comparison](evidence/metrics.csv) includes confusion counts. Random Forest scored three [fictional examples](data/new_patients.csv) as classes 0, 1, and 1, including an example with missing BMI. The output records the fitted training median of 29.0 used for that example. See the [prediction scores](evidence/predictions.csv) and [run output](evidence/output.txt). These predictions are classroom demonstrations based on survey labels.

Spark can distribute CSV reading, transformations, model training, and metric aggregation across partitions. Executors compute local statistics such as imputation summaries, gradient contributions, or split histograms, and Spark combines those statistics before the next algorithm step. The run output shows the training row count in each partition. Only summaries and the three fictional predictions are collected to the driver. [Spark MLlib pipeline guide](https://spark.apache.org/docs/3.5.8/ml-pipeline.html)
