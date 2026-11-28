"""Compile and run the Scala Spark lab using the compiler bundled with PySpark."""
import argparse
import os
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZIP_DEFLATED, ZipFile

import pyspark

HERE = Path(__file__).resolve().parent
DATA = HERE / "data" / "diabetes_brfss_2015.csv"


def main(args):
    spark_home = Path(pyspark.__file__).resolve().parent
    java_name = "java.exe" if os.name == "nt" else "java"
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin" / java_name) if java_home else java_name
    submit = spark_home / "bin" / ("spark-submit.cmd" if os.name == "nt" else "spark-submit")
    env = {**os.environ, "SPARK_HOME": str(spark_home)}
    jars = str(spark_home / "jars" / "*")

    with TemporaryDirectory(prefix="diabetes-scala-") as temp:
        build = Path(temp)
        classes = build / "classes"
        classes.mkdir()
        sources = sorted((HERE / "src").glob("*.scala"))
        subprocess.run([
            java, "-cp", jars, "scala.tools.nsc.Main", "-usejavacp", "-d", str(classes),
            *map(str, sources),
        ], check=True)
        if args.command == "prepare":
            subprocess.run([
                java, "-cp", os.pathsep.join((str(classes), jars)), "PrepareDataset",
                str(args.archive.resolve()), str(args.output.resolve()),
            ], check=True)
            return

        app = build / "diabetes.jar"
        with ZipFile(app, "w", ZIP_DEFLATED) as archive:
            for class_file in sorted(classes.rglob("*.class")):
                archive.write(class_file, class_file.relative_to(classes).as_posix())
        subprocess.run([
            str(submit), "--master", args.master, "--driver-memory", "2g",
            "--conf", f"spark.sql.shuffle.partitions={args.partitions}",
            "--class", "DiabetesPipeline", str(app),
            str(args.data.resolve()), str(args.patients.resolve()), str(args.output.resolve()),
        ], check=True, env=env)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    train = commands.add_parser("train", help="Run the Scala Spark MLlib pipeline")
    train.add_argument("--data", type=Path, default=DATA)
    train.add_argument("--patients", type=Path, default=HERE / "data" / "new_patients.csv")
    train.add_argument("--output", type=Path, default=HERE / "evidence")
    train.add_argument("--master", default="local[4]")
    train.add_argument("--partitions", type=int, default=4)
    prepare = commands.add_parser("prepare", help="Project the publisher's CSV in Scala")
    prepare.add_argument("archive", type=Path)
    prepare.add_argument("--output", type=Path, default=DATA)
    args = parser.parse_args()
    if args.command == "train" and args.partitions < 1:
        parser.error("--partitions must be at least 1")
    main(args)
