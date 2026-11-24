import argparse
import csv
from io import TextIOWrapper
from pathlib import Path
from zipfile import ZipFile

SOURCE_MEMBER = "diabetes_binary_5050split_health_indicators_BRFSS2015.csv"
FEATURES = (
    "HighBP", "HighChol", "BMI", "Smoker", "Stroke", "HeartDiseaseorAttack",
    "PhysActivity", "GenHlth", "DiffWalk", "Age",
)
LABEL = "Diabetes_binary"
DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "data" / "diabetes_brfss_2015.csv"


def prepare(archive_path, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(archive_path) as archive, archive.open(SOURCE_MEMBER) as source:
        reader = csv.DictReader(TextIOWrapper(source, encoding="utf-8-sig", newline=""))
        columns = (LABEL, *FEATURES)
        if not set(columns).issubset(reader.fieldnames or []):
            raise ValueError("The source archive is missing required columns.")
        with output_path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output, lineterminator="\n")
            writer.writerow(columns)
            writer.writerows([row[c] for c in columns] for row in reader)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Project the public BRFSS CSV to the lab columns.")
    parser.add_argument("archive", type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    prepare(args.archive, args.output)
