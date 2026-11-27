import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.SparkSession

import scala.io.Source

final case class Student(id: String, name: String, department: String, semester: Int, marks: Double)

object StudentPerformance {
  def main(args: Array[String]): Unit = {
    val path = args.headOption.getOrElse("assignment-1/data/students.csv")
    val source = Source.fromFile(path, "UTF-8")
    val students = try {
      val lines = source.getLines()
      require(lines.next() == "student_id,name,department,semester,marks", "Unexpected CSV header")
      lines.filter(_.trim.nonEmpty).map { line =>
        val fields = line.split(",", -1)
        require(fields.length == 5, s"Expected five columns: $line")
        Student(fields(0), fields(1), fields(2), fields(3).toInt, fields(4).toDouble)
      }.toVector
    } finally source.close()
    require(students.nonEmpty, "Student dataset is empty")

    val spark = SparkSession.builder.appName("BDA Assignment 1 - Student RDDs").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    try {
      // The driver splits the collection into input partitions. On a cluster, the cluster
      // manager allocates executors on worker nodes and the driver schedules tasks on them.
      val records = sc.parallelize(students, numSlices = 4)
      sc.setJobGroup("department-counts", "Count students by department")

      // An action triggers a job. The shuffle in reduceByKey divides this job into two stages.
      // In the first stage, one task per input partition maps students to department keys
      // and combines counts locally. The shuffle groups partial counts by department.
      // In the next stage, one task per output partition merges the counts for its keys.
      val departmentCounts = records.map(student => (student.department, 1)).reduceByKey(_ + _)
      println(s"Spark master: ${sc.master}")
      println("Department-count task distribution:")
      println(s"  Before the shuffle: ${records.getNumPartitions} tasks, one per input partition.")
      println(s"  After the shuffle: ${departmentCounts.getNumPartitions} tasks, one per output partition.")
      println("  On a cluster, the driver assigns these tasks to executors on worker nodes.")
      println("  Executors fetch the shuffle data for their partitions, and collect returns the final counts to the driver.")
      println(s"Jobs before action: ${sc.statusTracker.getJobIdsForGroup("department-counts").length}")
      println(s"RDD lineage for department counts:\n${departmentCounts.toDebugString}")
      println("Students per department:")
      departmentCounts.collect().sortBy(_._1).foreach(println)
      println(s"Jobs after collect: ${sc.statusTracker.getJobIdsForGroup("department-counts").length}")
      sc.clearJobGroup()

      println("All students:")
      records.collect().foreach(println)
      println("Students with marks above 75:")
      records.filter(_.marks > 75).collect().foreach(println)

      val marks = records.map(_.marks).stats()
      println(f"Average marks: ${marks.mean}%.2f")
      val highest = records.max()(Ordering.by[Student, Double](_.marks))
      println(s"Highest score: $highest")
      // This separate action observes actual task placement from inside each input partition.
      val partitionTasks = records.mapPartitions { partition =>
        val task = TaskContext.get()
        Iterator((task.partitionId(), SparkEnv.get.executorId, partition.size))
      }.collect().sortBy(_._1)
      println("Observed input tasks from the partition inspection job:")
      partitionTasks.foreach { case (partition, executor, size) =>
        println(s"  Partition $partition ran on executor $executor and processed $size records.")
      }
      if (sc.isLocal) {
        println("In local mode, Spark runs these partition tasks on threads in the driver process.")
      }
      println(s"Spark UI: ${sc.uiWebUrl.getOrElse("disabled")}")
      for (seconds <- args.lift(1).map(_.toLong) if seconds > 0) Thread.sleep(seconds * 1000)
    } finally spark.stop()
  }
}
