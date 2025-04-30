//> using scala 3.6.3
//> using dep org.typelevel::cats-core:2.13.0
//> using dep io.circe::circe-yaml:1.15.0
//> using dep io.circe::circe-generic:0.14.13
//> using dep io.circe::circe-parser:0.14.13
//> using dep com.networknt:json-schema-validator:1.5.6
//> using dep ch.qos.logback:logback-classic:1.5.18
//> using dep "dapla-kuben-resource-model:dapla-kuben-resource-model:1.0.3,url=https://github.com/statisticsnorway/dapla-kuben-resource-model/releases/download/java-v1.0.3/dapla-kuben-resource-model-1.0.3.jar"
//> using test.dep org.scalameta::munit::1.1.0
//> using files types.scala configUtils.scala utils.scala
package validate

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.networknt.schema.serialization.JsonNodeReader
import com.networknt.schema.SpecVersion.VersionFlag
import com.networknt.schema.*
import com.schemavalidation.types.{given, *}
import com.schemavalidation.config.utils.*
import com.schemavalidation.utils.*
import io.circe.*
import io.circe.syntax.*
import io.circe.yaml
import java.nio.file.{Files, Path, Paths}
import scala.collection.immutable.*
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.util.boundary, boundary.break
import no.ssb.dapla.kuben.v1.{SharedBucket, SharedBuckets}

/** Represents the different possible validation erros that can occur in a
  * delomaten configuration file.
  */
enum ValidationError:
  case SchemaValidationError(
      errorMessages: Set[ValidationMessage]
  )
  case NonExistantSharedBuckets(
      environment: String,
      sharedBucket: String,
      sharedBuckets: List[String]
  )
  case OverlappingPseudoTaskColumns(
      firstTaskName: String,
      secondTaskName: String,
      overlappingColumns: Set[String]
  )

/** Run all validations on the 'config.yaml' file and return a list of all
  * validation errors found.
  *
  * @param configDataPath
  *   the filepath to the 'config.yaml' file containing 'delomaten'
  *   configuration
  * @param contextualPath
  *   an artificial path of the form $environment/$folder/config.yaml used to
  *   provide better error messages
  * @param sharedBucketsPath
  *   the filepath to the shared-buckets buckets-shared.yaml file containing the
  *   teams' shared-buckets
  * @param environment
  *   the dapla team environment the source belongs to i.e. 'uh-varer-prod' or
  *   'play-obr-test'
  * @return
  *   a list of `ValidationError`s if any validation checks fail, otherwise an
  *   empty list
  */
def validateConfiguration(
    configDataPath: Path,
    contextualPath: Path,
    sharedBucketsPath: Path,
    environment: String
): List[ValidationError] =
  import ValidationError.*
  val delomaten: DelomatenConfig =
    loadConfig[DelomatenConfig](configDataPath) match
      case Left(ParsingFailure(msg, _)) =>
        throw Exception(s"Failed to parse delomaten config: $msg".red.newlines)
      case Left(error: DecodingFailure) =>
        throw Exception(
          s"Failed to decode delomaten config: $error".red.newlines
        )
      case Right(config) => config
  val sharedBuckets: SharedBuckets =
    loadConfig[SharedBuckets](sharedBucketsPath) match
      case Left(ParsingFailure(msg, _)) =>
        throw Exception(
          s"Failed to parse shared buckets config: $msg".red.newlines
        )
      case Left(error: DecodingFailure) =>
        throw Exception(
          s"Failed to decode shared buckets config: $error".red.newlines
        )
      case Right(config) => config

  val pseudoTargetedColumns: Set[String] =
    delomaten.pseudo.flatMap(_.columns).toSet

  val schemaValidationErrors = validateConfigSchema(configDataPath) match
    case errMessages if errMessages.nonEmpty =>
      Some(ValidationError.SchemaValidationError(errMessages))
    case _ => None

  // We have to check validation errors first, otherwise the
  // other validations may crash if they can't decode the
  // configuration properly
  schemaValidationErrors match
    case Some(validationError) => List(validationError)
    case None =>
      List(
        if !sharedBuckets
            .getBuckets()
            .asScala
            .toList
            .exists(bucket =>
              delomaten.sharedBucket `contains` bucket.getName()
            )
        then
          Some(
            NonExistantSharedBuckets(
              environment,
              delomaten.sharedBucket,
              sharedBuckets.getBuckets().asScala.toList.map(_.getName())
            )
          )
        else None,
        pseudoTaskColumnsUniquelyTargeted(delomaten.pseudo)
      ).flatten

/** Print error messags for validation errors
  *
  * @param validationErrors
  *   validation errors
  * @return
  *   Unit
  */
def printErrors(
    contextualPath: Path,
    validationErrors: List[ValidationError]
): Unit =
  import ValidationError.*
  validationErrors.foreach { valError =>
    valError match
      case SchemaValidationError(errorMessages) =>
        println(
          s"The delomaten configuration file '$contextualPath' is invalid:\n\n${errorMessages.mkString("\n")}".red.newlines
        )
      case NonExistantSharedBuckets(env, sharedBucket, sharedBuckets) =>
        println(s"""
        |In the configuration file '$contextualPath' in the field 'shared_bucket' the provided bucket '${sharedBucket}' does not exist.

        |Existing shared buckets for $env:
        |  ${sharedBuckets.map("- " + _).mkString("\n  ")}
        |""".stripMargin.red.newlines)
      case OverlappingPseudoTaskColumns(
            firstTaskName,
            secondTaskName,
            overlappingColumns
          ) =>
        println(s"""
        |In the configuration file '$contextualPath' the pseudo tasks '${firstTaskName}' and '${secondTaskName}' target overlapping columns.

        |Overlapping columns:
        |  ${overlappingColumns.map("- " + _).mkString("\n")}
        |""".stripMargin.red.newlines)
  }

/** Validate if the config.yaml under the @directoryPath is valid. Only uses
  * @environment
  *   and @folder arguments to print more informative logs.
  *
  * @param environment
  *   the dapla team environment the source belongs to i.e. 'uh-varer-prod' or
  *   'play-obr-test'
  * @param folder
  *   the statistics product folder which contains the configuration file i.e.
  *   'ledstill' or 'sykefra'
  * @param directoryPath
  *   the filepath to the config.yaml file containing 'delomaten' configuration
  * @param sharedBucketsPathStr
  *   the filepath to the shared-buckets buckets-shared.yaml file containing the
  *   teams' shared-buckets
  * @return
  *   Unit
  */
@main def runValidation(
    environment: String,
    folder: String,
    directoryPath: String,
    sharedBucketsPathStr: String
): Unit =

  if !Files.exists(Paths.get(directoryPath)) then
    println(
      s"The given directory path ${directoryPath} does not exist".red.newlines
    )
    System.exit(1)

  val configDataPath: Path = Paths.get(directoryPath, "config.yaml")
  val contextualPath: Path = Paths.get(environment, folder, "config.yaml")

  if !Files.exists(configDataPath) then
    val context = Paths.get(environment, folder)
    println(
      s"No 'config.yaml' file exists in the product source folder '${context}'".red.newlines
    )
    System.exit(1)

  val sharedBucketsPath: Path = Paths.get(sharedBucketsPathStr)

  if !Files.exists(sharedBucketsPath) then
    println(
      s"The given shared-buckets path '${sharedBucketsPath}' does not exist".red.newlines
    )
    System.exit(1)

  validateConfiguration(
    configDataPath,
    contextualPath,
    sharedBucketsPath,
    environment
  ) match
    case validationErrors if validationErrors.nonEmpty =>
      printErrors(contextualPath, validationErrors)
      System.exit(1)
    case _ => ()

  println(
    s"The '${contextualPath}' configuration was successfully validated!".green.newlines
  )

import ValidationError.OverlappingPseudoTaskColumns
// Ensure that the pseudo task columns are only targeted once.
def pseudoTaskColumnsUniquelyTargeted(
    pseudoTasks: List[PseudoTask]
): Option[OverlappingPseudoTaskColumns] =
  val assocMap: Map[String, List[String]] =
    pseudoTasks.map(task => task.name -> task.columns).toMap

  boundary[Option[OverlappingPseudoTaskColumns]]:
    for (taskName, targetedColumns) <- assocMap do
      val remainingMap = assocMap - taskName
      for (taskNameB, targetedColumnsB) <- remainingMap do
        val overlappingColumns: Set[String] =
          targetedColumns.toSet & targetedColumnsB.toSet
        if overlappingColumns.nonEmpty then
          break[Option[OverlappingPseudoTaskColumns]](
            Some(
              OverlappingPseudoTaskColumns(
                taskName,
                taskNameB,
                overlappingColumns
              )
            )
          )
    None

def validateConfigSchema(filepath: Path): Set[ValidationMessage] =
  val schemaData: String =
    Source.fromFile("./config_schema_spec.yaml").getLines().mkString("\n")
  val inputData: String = Source
    .fromFile(filepath.toFile())
    .getLines()
    .mkString("\n")

  val yamlMapper: ObjectMapper = ObjectMapper(YAMLFactory())
  val jsonNodeReader: JsonNodeReader =
    JsonNodeReader.builder().yamlMapper(yamlMapper).build()
  val config: SchemaValidatorsConfig =
    SchemaValidatorsConfig.builder().errorMessageKeyword("errorMessage").build()
  val schema: JsonSchema = JsonSchemaFactory
    .getInstance(
      VersionFlag.V202012,
      builder => builder.jsonNodeReader(jsonNodeReader).build()
    )
    .getSchema(schemaData, InputFormat.YAML, config)
  schema
    .validate(
      inputData,
      InputFormat.YAML,
      (executionContext) =>
        executionContext.getExecutionConfig().setFormatAssertionsEnabled(true)
    )
    .asScala
    .toSet
