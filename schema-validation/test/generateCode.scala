//> using files ../src/utils.scala
import java.nio.file.{Files, Path, Paths}
import scala.sys.process.*
import scala.jdk.CollectionConverters.*
import com.schemavalidation.utils.*

/** Generates some python code from valid YAML configurations and ensures that
  * they don't contain programatic errors using flake8.
  */
@main def testCodegenForValidConfigurations(): Unit =
  val basePath = Paths.get("schema-validation/test/schema-test-data")
  val files = Files.newDirectoryStream(
    basePath,
    { (p: Path) =>
      val filename = p.getFileName().toString
      filename.startsWith("valid_data") && filename.endsWith(".yaml")
    }
  )

  files.asScala.foreach { (path: Path) =>
    val filepath = path.toString
    val outputFileName = path.getFileName.toString.stripSuffix(".yaml") ++ ".py"
    val genPythonCode = Seq(
      "scala-cli",
      "run",
      "schema-validation/src/generateCode.scala",
      "--",
      filepath,
      outputFileName
    )
    val genPythonCodeExitCode = genPythonCode.!
    if genPythonCodeExitCode != 0
    then
      println(s"Failed to generate python code for $filepath file")
      sys.exit(genPythonCodeExitCode)

    val testPythonCode = Seq("flake8", "--config", "flake8conf", outputFileName)
    val testPythonCodeExitCode = testPythonCode.!

    if testPythonCodeExitCode != 0
    then
      println(s"Failed to validate python code in $outputFileName")
      sys.exit(testPythonCodeExitCode)

    println("Successfully validated python code".green)
  }
