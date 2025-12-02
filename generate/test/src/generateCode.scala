import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable.ListBuffer
import scala.sys.process.*
import scala.jdk.CollectionConverters.*
import com.schemavalidation.utils.*
import generate.Main
import java.nio.file.DirectoryStream

class SchemaValidationTests extends munit.FunSuite:
  // Since mill runs tests inside a sandboxed filepath we need to retrieve the base path
  // when reading files.
  val workspaceRoot: Path = Paths.get(sys.env("MILL_WORKSPACE_ROOT"))

  // Get all files from a location in a directory with a given `prefix` and file `extension`
  def getFiles(
      location: String,
      prefix: String,
      extension: String
  ): DirectoryStream[java.nio.file.Path] =
    Files.newDirectoryStream(
      workspaceRoot.resolve(s"schema-validation/test/data/${location}"),
      { (p: Path) =>
        val filename = p.getFileName().toString
        filename.startsWith(prefix) && filename.endsWith(extension)
      }
    )

  /** Generates some python code from valid YAML configurations and ensures that
    * they don't contain programatic errors using ruff check.
    */
  test("Code generation for valid configurations passes ruff check.") {
    val files = getFiles("schema-validation", "valid_data", ".yaml")

    files.asScala.foreach { (path: Path) =>
      val projectDisplayName = "test-project-prod"
      val filepath = path.toString
      val outputFileName =
        path.getFileName.toString.stripSuffix(".yaml") ++ ".py"
      Main.generateCode(projectDisplayName, filepath, Some(outputFileName))

      val testPythonCode =
        Seq("ruff", "check", outputFileName)

      val stdoutBuffer = ListBuffer[String]()
      val logger = ProcessLogger(out => stdoutBuffer += out)
      val testPythonCodeExitCode = testPythonCode.!(logger)
      assert(
        testPythonCodeExitCode == 0,
        s"Failed to validate python code in $outputFileName\n\n${stdoutBuffer.mkString("\n")}".red
      )

      println("Successfully validated python code".green)
    }
  }

  test(
    "Code generation for valid configurations produces expected python code"
  ) {
    val workspace = workspaceRoot.resolve("schema-validation/test/data/golden")
    val configFiles = getFiles("golden", "valid_config", ".yaml")
    val pythonFiles = getFiles("golden", "process_shared_data", ".yaml")

    configFiles.asScala.foreach { (path: Path) =>
      val projectDisplayName = "dapla-team-prod"
      val configFile = Files.readString(path)
      val index: Int =
        "(\\d+)".r.findFirstIn(path.getFileName.toString).get.toInt
      val expectedCode = Files.readString(
        path.getParent().resolve(s"process_shared_data${index}.py")
      )

      val generatedCode = Main.generateCode(projectDisplayName, path.toString)

      assertNoDiff(
        generatedCode,
        expectedCode,
        "The generated python code doesn't match the expected code".red
      )

      println(
        "Successfully matched generated python code with excpeted code".green
      )
    }
  }
