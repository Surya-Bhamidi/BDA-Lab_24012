import java.io.InputStreamReader
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.zip.ZipFile

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings, CsvWriter, CsvWriterSettings}

object PrepareDataset {
  val Features: Seq[String] = Seq(
    "HighBP", "HighChol", "BMI", "Smoker", "Stroke", "HeartDiseaseorAttack",
    "PhysActivity", "GenHlth", "DiffWalk", "Age"
  )
  val Label: String = "Diabetes_binary"
  private val member = "diabetes_binary_5050split_health_indicators_BRFSS2015.csv"

  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Usage: PrepareDataset <upstream.zip> <output.csv>")
    val destination = Paths.get(args(1)).toAbsolutePath.normalize()
    Files.createDirectories(destination.getParent)
    val temporary = Files.createTempFile(destination.getParent, "brfss-", ".csv")
    try {
      val archive = new ZipFile(args(0))
      try {
        val entry = archive.getEntry(member)
        require(entry != null, s"The upstream archive must contain $member")
        val input = new InputStreamReader(archive.getInputStream(entry), UTF_8)
        val parserSettings = new CsvParserSettings()
        parserSettings.setLineSeparatorDetectionEnabled(true)
        val parser = new CsvParser(parserSettings)
        try {
          parser.beginParsing(input)
          val header = Option(parser.parseNext()).getOrElse(Array.empty[String])
          val fields = Label +: Features
          val indices = fields.map(field => header.indexWhere(_.stripPrefix("\uFEFF") == field))
          require(indices.forall(_ >= 0), "The upstream CSV is missing required columns")

          val settings = new CsvWriterSettings()
          settings.getFormat.setLineSeparator("\n")
          val output = new CsvWriter(Files.newBufferedWriter(temporary, UTF_8), settings)
          try {
            output.writeRow(fields.map(_.asInstanceOf[AnyRef]): _*)
            var row = parser.parseNext()
            while (row != null) {
              require(indices.forall(_ < row.length), "The upstream CSV contains a truncated data row")
              output.writeRow(indices.map(i => row(i).asInstanceOf[AnyRef]): _*)
              row = parser.parseNext()
            }
          } finally output.close()
        } finally {
          parser.stopParsing()
          input.close()
        }
      } finally archive.close()
      Files.move(temporary, destination, StandardCopyOption.REPLACE_EXISTING)
    } finally Files.deleteIfExists(temporary)
  }
}
