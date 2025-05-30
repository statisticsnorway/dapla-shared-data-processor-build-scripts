//> using scala 3.6.3
//> using dep org.typelevel::cats-core:2.13.0
//> using dep io.circe::circe-yaml:1.15.0
//> using dep io.circe::circe-generic:0.14.13
//> using dep io.circe::circe-parser:0.14.13
//> using files types.scala configUtils.scala utils.scala
//> using dep "dapla-kuben-resource-model:dapla-kuben-resource-model:1.0.3,url=https://github.com/statisticsnorway/dapla-kuben-resource-model/releases/download/java-v1.0.3/dapla-kuben-resource-model-1.0.3.jar"

import com.schemavalidation.types.{given, *}
import com.schemavalidation.config.utils.*
import com.schemavalidation.utils.*
import java.nio.file.{Files, Path, Paths}
import scala.util.{Try, Using}
import java.io.FileWriter
import java.io.BufferedWriter
import java.text.SimpleDateFormat
import DataFrameString.*
import io.circe.syntax.*
import io.circe.Json

/** Represents a python variable referencing the dataframe being operated on or
  * a value of type Result from dapla-toolbelt-pseudo
  */
enum DataFrameString:
  case DataFrame
  case Result

/** Generate 'delomaten' python code from a yaml configuration.
  *
  * @param configDataPath
  *   The filepath to the yaml configuration file.
  * @param writeFilepath
  *   An optional filepath for the output file.
  *
  * @return
  *   Unit
  */
// HACK: The [[scala.util.CommandLineParser]] abstraction doesn't support
// optional positional arguments so @writeFilepath is expressed
// as a list of arguments instead.
// TODO: Replace this HACK with `https://github.com/com-lihaoyi/mainargs` library
@main def generateCode(configDataPath: String, writeFilepath: String*): Unit =
  val configPath: Path = Paths.get(configDataPath)
  if !Files.exists(configPath) then
    println(
      s"The configuration file at ${configDataPath} does not exist".red.newlines
    )

  val config: DelomatenConfig = loadConfig[DelomatenConfig](configPath) match
    case Left(err) =>
      throw Exception("Unexpected error, couldn't load Delomaten config")
    case Right(config) => config

  val pseudoTasks: String =
    config.pseudo
      .groupBy(_.pseudoOperation)
      .map(genPseudoTask)
      .zip(LazyList(DataFrame) ++ LazyList.continually(Result))
      .map { case (f, s) => f(s) }
      .mkString("\n\n")

  val code = templateCode(config, pseudoTasks)

  println("Python code generated successfully".green.newlines)
  println(code.yellow)

  writeFile(
    if writeFilepath.nonEmpty
    then writeFilepath.head
    else "process_shared_data.py",
    code
  )

def writeFile(filename: String, content: String): Try[Unit] =
  Using(BufferedWriter(FileWriter(Paths.get(filename).toFile(), false))) {
    bufferedWriter =>
      bufferedWriter.write(content)
  }

def templateCode(
    config: DelomatenConfig,
    code: String
): String =
  s"""from dapla_pseudo import Depseudonymize, Pseudonymize, Repseudonymize
    |from datetime import date
    |import logging
    |from google.cloud import storage
    |import io
    |from pathlib import Path
    |import polars as pl
    |import sys
    |import json

    |def main(file_path):
    |    try:
    |        df = pl.read_parquet(file_path)
    |    except Exception as e:
    |        logging.error(f"Failed to read {file_path} from parquet into dataframe\\n\\n{e}")
    |        sys.exit(1)
    |
    |${code}
    |    metrics = json.dumps(result.metadata_details, indent=4)
    |    logging.info("Metrics metadata %s", metrics)
    |    final_df = result.to_polars()

    |    client = storage.Client()
    |    bucket = client.bucket("${config.sharedBucket}")
    |    filename = Path(file_path).name
    |    filename_metrics = f"{Path(file_path).stem}_METRICS.json"
    |    blob_df = bucket.blob(str(Path("${config.destinationFolder}", filename)))
    |    blob_metrics = bucket.blob(str(Path("${config.destinationFolder}", filename_metrics)))
    |
    |    buffer = io.BytesIO()
    |    final_df.write_parquet(buffer)
    |    buffer.seek(0)

    |    blob_df.upload_from_file(buffer, content_type="application/octet-stream")
    |    logging.info(f"Result uploaded to ${config.sharedBucket}/${config.destinationFolder}/{filename}")
    |    blob_metrics.upload_from_string(metrics, content_type="application/json")
    |    logging.info(f"Metrics uploaded to ${config.sharedBucket}/${config.destinationFolder}/{filename_metrics}")
  """.stripMargin

def genPseudoTask(
    pseudoOperation: PseudoOperation,
    tasks: List[PseudoTask]
): DataFrameString => String =
  val pseudoOp = pseudoOperation match
    case PseudoOperation.Depseudo => "Depseudonymize"
    case PseudoOperation.Pseudo   => "Pseudonymize"

  val taskBlocks = tasks.map(genTaskBlock).mkString("\n")

  dataFrame =>
    val fromType = dataFrame match
      case DataFrame => "polars"
      case Result    => "result"
    val dataFrameString = dataFrame match
      case DataFrame => "df"
      case Result    => "result"
    s"""    result = (
    |      ${pseudoOp}
    |        .from_${fromType}(${dataFrameString})
    |${taskBlocks}
    |        .run()
    |    )""".stripMargin

def genTaskBlock(task: PseudoTask): String =
  import EncryptionAlgorithm.*
  val encryptAlgo = task.encryption match
    case Default(key) => s"with_default_encryption(custom_key=${key.asJson})"
    case PapisCompatible(key) =>
      s"with_papis_compatible_encryption(custom_key=${key.asJson})"
    case SidMapping(key, sidSnapshotDate, sidOnMapFailure) =>
      val date: Json =
        sidSnapshotDate.map(_.asJson).getOrElse(Json.fromString("date.today()"))
      val mapStrat: Json = sidOnMapFailure
        .map(_.asJson)
        .getOrElse(SidMapFailureStrategy.ReturnNull.asJson)
      s"with_stable_id(custom_key=${key.asJson}, sid_snapshot_date=${date}, on_map_failure=${mapStrat})"

  val columns = task.columns.map(col => s"\"$col\"").mkString(",")

  s"""        .on_fields(${columns})
  |        .${encryptAlgo}""".stripMargin
