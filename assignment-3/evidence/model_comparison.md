# Assignment 3 model comparison

Accuracy, precision, recall, and F1 were measured on the same 14,193 held-out rows.
Precision, recall, and F1 refer to label 1 (prediabetes or diabetes).

| Model | Validation F1 | Accuracy | Precision | Recall | Test F1 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Logistic Regression | 0.7461 | 0.7432 | 0.7332 | 0.7625 | 0.7476 |
| Decision Tree | 0.7433 | 0.7387 | 0.7135 | 0.7954 | 0.7522 |
| Random Forest | 0.7520 | 0.7449 | 0.7188 | 0.8025 | 0.7583 |

## Interpretation

Random Forest was selected for the fictional patient predictions because it had the highest validation F1 (0.7520). F1 is the harmonic mean of precision and recall.

Random Forest had the highest test F1 (0.7583). The selected model detected 80.2% of actual positives, and 71.9% of its positive predictions were correct. Logistic Regression had the highest test precision (73.3%).

These scores describe performance on this balanced survey dataset.

[Metrics and confusion counts](metrics.csv) | [Fictional patient predictions](predictions.csv)
