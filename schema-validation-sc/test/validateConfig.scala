//> using scala 3.6.3
//> using dep org.scalameta::munit::1.1.0
//> using dep com.networknt:json-schema-validator:1.5.5
//> using dep ch.qos.logback:logback-classic:1.5.16

import scala.io.Source
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.networknt.schema.serialization.JsonNodeReader
import com.networknt.schema.SpecVersion.VersionFlag
import com.networknt.schema.*
import scala.jdk.CollectionConverters.*

enum SchemaType:
  case Config(filepath: String)
  case Meta

class SchemaValidationTests extends munit.FunSuite:
  val schemaData: String =
    Source.fromFile("./config_schema.yaml").getLines().mkString("\n")

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
        Source.fromFile(filepath).getLines().mkString("\n")
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

  test("Is config_schema.yaml a valid YAML schema specification?") {
    val messages = validateYaml(SchemaType.Meta)
    assertEquals(messages.size, 0)
  }

  test("Is valid_data.yaml a valid configuration?") {
    val messages =
      validateYaml(SchemaType.Config("./test/test_data/valid_data.yaml"))
    assertEquals(messages.size, 0)
  }

  test("Invalid yaml configurations fail the tests") {
    (1 to 5).foreach { i =>
      val messages =
        validateYaml(SchemaType.Config(s"./test/test_data/invalid_data$i.yaml"))
      assert(clue(messages.size) > clue(0))
    }
  }
