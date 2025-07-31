import java.nio.file.{Files, Path, Paths}
import scala.sys.process.*
import scala.jdk.CollectionConverters.*
import com.schemavalidation.utils.*
import generate.Main

class SchemaValidationTests extends munit.FunSuite:
  // Since mill runs tests inside a sandboxed filepath we need to retrieve the base path
  // when reading files.
  val workspaceRoot: Path = Paths.get(sys.env("MILL_WORKSPACE_ROOT"))

  /** Generates some python code from valid YAML configurations and ensures that
    * they don't contain programatic errors using flake8.
    */
  test("Code generation for valid configurations are valid.") {
    val files = Files.newDirectoryStream(
      workspaceRoot.resolve("schema-validation/test/data/schema-validation"),
      { (p: Path) =>
        val filename = p.getFileName().toString
        filename.startsWith("valid_data") && filename.endsWith(".yaml")
      }
    )

    files.asScala.foreach { (path: Path) =>
      val filepath = path.toString
      val outputFileName =
        path.getFileName.toString.stripSuffix(".yaml") ++ ".py"
      Main.generateCode(filepath, Some(outputFileName))

      val testPythonCode =
        Seq("flake8", "--config", "flake8conf", outputFileName)
      val testPythonCodeExitCode = testPythonCode.!
      assert(
        testPythonCodeExitCode != 0,
        s"Failed to validate python code in $outputFileName"
      )

      println("Successfully validated python code".green)
    }
  }
