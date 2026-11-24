import argparse
import os
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZIP_DEFLATED, ZipFile

import pyspark

HERE = Path(__file__).resolve().parent


def main(args):
    spark_home = Path(pyspark.__file__).resolve().parent
    java = Path(os.environ["JAVA_HOME"]) / "bin" / ("java.exe" if os.name == "nt" else "java") if "JAVA_HOME" in os.environ else "java"
    submit = spark_home / "bin" / ("spark-submit.cmd" if os.name == "nt" else "spark-submit")
    env = os.environ.copy()
    env["SPARK_HOME"] = str(spark_home)

    with TemporaryDirectory(prefix="student-rdd-") as temp:
        build = Path(temp)
        classes = build / "classes"
        classes.mkdir()
        subprocess.run([
            str(java), "-cp", str(spark_home / "jars" / "*"),
            "scala.tools.nsc.Main", "-usejavacp", "-d", str(classes),
            str(HERE / "src" / "StudentPerformance.scala"),
        ], check=True)
        app = build / "students.jar"
        with ZipFile(app, "w", ZIP_DEFLATED) as archive:
            for class_file in sorted(classes.rglob("*.class")):
                archive.write(class_file, class_file.relative_to(classes).as_posix())
        subprocess.run([
            str(submit), "--master", args.master,
            "--conf", f"spark.ui.port={args.ui_port}",
            "--class", "StudentPerformance", str(app),
            str(args.data.resolve()), str(args.ui_wait),
        ], check=True, env=env)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compile and submit the Scala RDD lab.")
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--data", type=Path, default=HERE / "data" / "students.csv")
    parser.add_argument("--ui-port", type=int, default=4040)
    parser.add_argument("--ui-wait", type=int, default=0, help="Seconds to leave the Spark UI open")
    main(parser.parse_args())
