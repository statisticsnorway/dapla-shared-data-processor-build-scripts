import scala.io.Source
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.networknt.schema.serialization.JsonNodeReader
import com.networknt.schema.SpecVersion.VersionFlag
import com.networknt.schema.*
import scala.jdk.CollectionConverters.*
import validate.{ValidationError, validateConfiguration}
import java.nio.file.{Files, Path, Paths}

enum SchemaType:
  case Config(filepath: Path)
  case Meta

class SchemaValidationTests extends munit.FunSuite:
  // Since mill runs tests inside a sandboxed filepath we need to retrieve the base path
  // when reading files.
  val workspaceRoot: Path =
    Paths.get(sys.env("MILL_WORKSPACE_ROOT"), "schema-validation")
  def schemaData: String =
    Source
      .fromFile(workspaceRoot.resolve("config_schema_spec.yaml").toString)
      .getLines()
      .mkString("\n")

  def validateYaml(schemaType: SchemaType): Set[ValidationMessage] =
    val yamlMapper: ObjectMapper = ObjectMapper(YAMLFactory())
    val jsonNodeReader: JsonNodeReader =
      JsonNodeReader.builder().yamlMapper(yamlMapper).build()
    val config: SchemaValidatorsConfig = SchemaValidatorsConfig
      .builder()
      .errorMessageKeyword("errorMessage")
      .build()
    val schema: JsonSchema = schemaType match
      case SchemaType.Config(_) =>
        JsonSchemaFactory
          .getInstance(
            VersionFlag.V202012,
            builder => builder.jsonNodeReader(jsonNodeReader).build()
          )
          .getSchema(schemaData, InputFormat.YAML, config)
      case SchemaType.Meta =>
        JsonSchemaFactory
          .getInstance(VersionFlag.V202012)
          .getSchema(SchemaLocation.of(SchemaId.V202012), config)
    val data = schemaType match
      case SchemaType.Config(filepath) =>
        Source.fromFile(filepath.toString).getLines().mkString("\n")
      case SchemaType.Meta => schemaData
    schema
      .validate(
        data,
        InputFormat.YAML,
        (executionContext) =>
          executionContext.getExecutionConfig().setFormatAssertionsEnabled(true)
      )
      .asScala
      .toSet

  test("Is config_schema_spec.yaml a valid YAML schema specification?") {
    val messages = validateYaml(SchemaType.Meta)
    assertEquals(messages.size, 0)
  }

  test("Yaml configurations are valid according to the schema specification") {
    (1 to 4).foreach { i =>
      val messages =
        validateYaml(
          SchemaType.Config(
            workspaceRoot.resolve(
              s"test/data/schema-validation/valid_data$i.yaml"
            )
          )
        )
      assert(clue(messages.size) == clue(0))
    }
  }

  test(
    "Yaml configurations are invalid according to the schema specification"
  ) {
    (1 to 6).foreach { i =>
      val messages =
        validateYaml(
          SchemaType.Config(
            workspaceRoot.resolve(
              s"test/data/schema-validation/invalid_data$i.yaml"
            )
          )
        )
      assert(clue(messages.size) > clue(0))
    }
  }

  val configurationFixture = FunFixture[(Path, Path, String)](
    setup = { _testOptions =>
      (
        Paths.get("test/validation-tests/config.yaml"),
        workspaceRoot.resolve(
          "test/data/programatic-validation/buckets-shared.yaml"
        ),
        "test"
      )
    },
    teardown = identity
  )

  import ValidationError.*

  configurationFixture.test(
    "Valid yaml configurations don't return programatic validation errors"
  ) { (contextualPath, sharedBucketsPath, environment) =>
    (1 to 3).foreach { i =>
      val configDataPath: Path =
        workspaceRoot.resolve(
          s"test/data/programatic-validation/valid_config$i.yaml"
        )

      val validationErrors: List[ValidationError] = validateConfiguration(
        configDataPath,
        contextualPath,
        sharedBucketsPath,
        environment
      )

      assert(validationErrors.isEmpty)
    }
  }

  configurationFixture.test(
    "Invalid yaml configuration returns schema validation error"
  ) { (contextualPath, sharedBucketsPath, environment) =>
    val configDataPath: Path =
      workspaceRoot.resolve(
        "test/data/programatic-validation/invalid_schema.yaml"
      )
    val validationErrors: List[ValidationError] = validateConfiguration(
      configDataPath,
      contextualPath,
      sharedBucketsPath,
      environment
    )
    assert(
      validationErrors.exists { err =>
        err match
          case _: SchemaValidationError => true
          case _                        => false
      }
    )
  }

  configurationFixture.test(
    "Invalid yaml configuration returns shared buckets validation error"
  ) { (contextualPath, sharedBucketsPath, environment) =>
    val configDataPath: Path =
      workspaceRoot.resolve(
        "test/data/programatic-validation/invalid_shared_buckets.yaml"
      )
    val validationErrors: List[ValidationError] = validateConfiguration(
      configDataPath,
      contextualPath,
      sharedBucketsPath,
      environment
    )
    assert(
      validationErrors.exists { err =>
        err match
          case _: NonExistantSharedBuckets => true
          case _                           => false
      }
    )
  }

  configurationFixture.test(
    "Invalid yaml configuration returns overlapping pseudo task columns validation error"
  ) { (contextualPath, sharedBucketsPath, environment) =>
    val configDataPath: Path =
      workspaceRoot.resolve(
        "test/data/programatic-validation/invalid_pseudo_task_columns.yaml"
      )
    val validationErrors: List[ValidationError] = validateConfiguration(
      configDataPath,
      contextualPath,
      sharedBucketsPath,
      environment
    )
    assert(
      validationErrors.exists { err =>
        err match
          case _: OverlappingPseudoTaskColumns => true
          case _                               => false
      }
    )
  }

  configurationFixture.test(
    "Invalid yaml configuration returns validation error for depseudo operations"
  ) { (contextualPath, sharedBucketsPath, environment) =>
    val configDataPath: Path =
      workspaceRoot.resolve(
        "test/data/programatic-validation/invalid_depseudo_operations.yaml"
      )
    val validationErrors: List[ValidationError] = validateConfiguration(
      configDataPath,
      contextualPath,
      sharedBucketsPath,
      environment
    )
    assert(
      validationErrors.exists { err =>
        err match
          case _: SchemaValidationError => true
          case _                        => false
      }
    )
  }
