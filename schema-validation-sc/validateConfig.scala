//> using scala 3.6.3
//> using dep org.typelevel::cats-core:2.13.0
//> using dep io.circe::circe-yaml:1.15.0
//> using dep io.circe::circe-generic:0.14.10
//> using dep io.circe::circe-parser:0.14.10
//> using dep com.networknt:json-schema-validator:1.5.5
//> using dep ch.qos.logback:logback-classic:1.5.16
package validate

import cats.syntax.all.*
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.networknt.schema.serialization.JsonNodeReader
import com.networknt.schema.SpecVersion.VersionFlag
import com.networknt.schema.*
import io.circe.*
import io.circe.derivation.*
import io.circe.generic.semiauto.*
import io.circe.syntax.*
import io.circe.yaml
import io.circe.derivation.Configuration
import java.nio.file.{Files, Path, Paths}
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.collection.immutable.*

// Helper extensions for String type
extension (str: String)
  def red = str.split("\n").map(Console.RED + _).mkString("\n")
  def green = str.split("\n").map(Console.GREEN + _).mkString("\n")
  def newlines = s"\n\n$str\n\n"

enum PseudoOperation:
  case Pseudo, Depseudo, Redact

given Decoder[PseudoOperation] = ConfiguredEnumDecoder.derive(_.toUpperCase)
given Encoder[PseudoOperation] = ConfiguredEnumEncoder.derive(_.toUpperCase)

enum EncryptionAlgorithm:
  case PapisCompatible, SidMapping

given Decoder[EncryptionAlgorithm] = ConfiguredEnumDecoder.derived(using
  Configuration.default.withSnakeCaseConstructorNames
)

case class PseudoTask(
    name: String,
    columns: List[String],
    pseudoOperation: PseudoOperation,
    encryptionAlgorithm: EncryptionAlgorithm,
    encryptionArgs: Option[List[String]]
)
given Decoder[PseudoTask] = ConfiguredDecoder.derived(using
  Configuration.default.withSnakeCaseMemberNames
)

case class DelomatenConfig(
    sharedBucket: String,
    sourceFolder: String,
    destinationFolder: String,
    memorySize: Int,
    pseudo: List[PseudoTask],
    outputColumns: Option[List[String]]
)
given Decoder[DelomatenConfig] = ConfiguredDecoder.derived(using
  Configuration.default.withSnakeCaseMemberNames
)

case class SharedBuckets(
    buckets: HashMap[String, HashMap[String, List[String]]]
)
given Decoder[SharedBuckets] = deriveDecoder

/** Load YAML configuration and decode it into scala type
  */
def loadConfig[A](path: Path)(using decoder: Decoder[A]): A =
  val configData = Source.fromFile(path.toFile()).getLines().mkString("\n")

  yaml.parser
    .parse(configData)
    .leftMap(err => err: Error)
    .flatMap(_.as(decoder)) match
    case Left(err)     => throw Exception(s"Error: ${err.getMessage()}")
    case Right(config) => config

/** Validate if the config.yaml under the @directory_path is valid. Only uses
  * \@environment and @folder arguments to print more informative logs.
  *
  * @param environment
  *   the dapla team environment the source belongs to i.e. 'uh-varer-prod' or
  *   'play-obr-test'
  * @param folder
  *   the statistics product folder which contains the configuration file i.e.
  *   'ledstill' or 'sykefra'
  * @param directory_path
  *   the filepath to the config.yaml file containing 'delomaten' configuration
  * @param shared_buckets_path
  *   the filepath to the shared-buckets iam.yaml file containing the teams'
  *   shared-buckets
  * @return
  *   Unit
  */
@main def validateConfig(
    environment: String,
    folder: String,
    directoryPath: String,
    sharedBucketsPath: String
): Unit =

  if !Files.exists(Paths.get(directoryPath)) then
    println(
      s"The given directory path ${directoryPath} does not exist".red.newlines
    )
    System.exit(1)

  val configDataPath: Path = Paths.get(directoryPath, "config.yaml")

  if !Files.exists(configDataPath) then
    val context = Paths.get(environment, folder).toString()
    println(
      s"No 'config.yaml' file exists in the product source folder '${context}'".red.newlines
    )
    System.exit(1)

  if !Files.exists(Paths.get(sharedBucketsPath)) then
    println(
      s"The given shared-buckets path '${sharedBucketsPath}' does not exist".red.newlines
    )
    System.exit(1)

  validateConfigSchema(configDataPath) match
    case Left(errMessages) =>
      val contextualPath = Paths.get(environment, folder, "config.yaml")
      println(
        s"The delomaten configuration file '${contextualPath}' is invalid:\n\n${errMessages.mkString("\n")}".red.newlines
      )
    case Right(_) => ()

  val delomaten: DelomatenConfig = loadConfig(configDataPath)
  val sharedBuckets: SharedBuckets = loadConfig(Paths.get(sharedBucketsPath))

  // If the shared bucket specified in the config.yaml doesn't exist in the dapla team, report an error
  if !sharedBuckets.buckets.keys.toSet.contains(delomaten.sharedBucket) then
    println(s"""
      |In the configuration file "${configDataPath}" in the field "shared_bucket" the provided bucket "${delomaten.sharedBucket}" does not exist.

      |Existing shared buckets for ${environment}:
      |  ${sharedBuckets.buckets.keys.map("- " + _).mkString("\n")}
    """.stripMargin.red.newlines)
    System.exit(1)

  val pseudoTargetedColumns: Set[String] =
    delomaten.pseudo.flatMap(_.columns).toSet

  delomaten.outputColumns match
    case Some(columns) if (pseudoTargetedColumns &~ columns.toSet).size > 0 =>
      val diff = pseudoTargetedColumns &~ columns.toSet
      println(s"""
          |In the configuration file '${configDataPath}' in the field 'output_columns'
          |not all columns targeted by pseudo operations are listed in the 'output_columns'.

          |The missing columns are:
          |  ${diff.map("- " + _).mkString("\n")}
        """.stripMargin.red.newlines)
      System.exit(1)
    case _ => ()

  pseudoTaskColumnsUniquelyTargeted(delomaten.pseudo)

  println(
    s"The '${configDataPath}' configuration was successfully validated!".green.newlines
  )

// Ensure that the pseudo task columns are only targeted once.
def pseudoTaskColumnsUniquelyTargeted(pseudoTasks: List[PseudoTask]): Unit =
  val assocMap: Map[String, List[String]] =
    pseudoTasks.map(task => task.name -> task.columns).toMap

  for (taskName, targetedColumns) <- assocMap do
    val remainingMap = assocMap - taskName
    for (taskNameB, targetedColumnsB) <- remainingMap do
      val overlappingColumns = Set(targetedColumns) & Set(targetedColumnsB)
      if overlappingColumns.size > 0 then
        println(
          s"""
            |The pseudo tasks '${taskName}' and '${taskNameB}' target overlapping columns.

            |Overlapping columns:
            |  ${overlappingColumns.map("- " + _).mkString("\n")}
          """.stripMargin.red.newlines
        )
        System.exit(1)

def validateConfigSchema(filepath: Path): Either[Set[ValidationMessage], Unit] =
  val schemaData: String =
    Source.fromFile("./config_schema.yaml").getLines().mkString("\n")
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
  val messages: Set[ValidationMessage] = schema
    .validate(
      inputData,
      InputFormat.YAML,
      (executionContext) =>
        executionContext.getExecutionConfig().setFormatAssertionsEnabled(true)
    )
    .asScala
    .toSet

  if messages.size == 0 then Right(())
  else Left(messages)
