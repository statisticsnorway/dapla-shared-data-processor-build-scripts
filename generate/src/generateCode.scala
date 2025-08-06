package generate

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
object Main:
  def generateCode(
      projectDisplayName: String,
      configDataPath: String,
      writeFilepath: Option[String] = None
  ): Unit =
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

    val (projectName, environment) = splitProjectDisplayName(projectDisplayName)
    val code = templateCode(projectName, environment, config, pseudoTasks)

    println("Python code generated successfully".green.newlines)
    println(code.yellow)

    writeFile(
      writeFilepath.getOrElse("process_shared_data.py"),
      code
    )

  def main(args: Array[String]): Unit =
    args match
      case Array(projectDisplayName, configDataPath) => generateCode(projectDisplayName, configDataPath)
      case Array(projectDisplayName, configDataPath, writeFilepath) =>
        generateCode(projectDisplayName, configDataPath, Some(writeFilepath))

/* Split project display name into project name and environment
 *
 * Example:
 *   "play-obr-test" -> ("play-obr", "test")
 */
def splitProjectDisplayName(projectDisplayName: String): (String, String) =
  val splitIndex = projectDisplayName.lastIndexOf("-")
  (projectDisplayName.substring(0, splitIndex ), projectDisplayName.substring(splitIndex + 1))

def writeFile(filename: String, content: String): Try[Unit] =
  Using(BufferedWriter(FileWriter(Paths.get(filename).toFile(), false))) {
    bufferedWriter =>
      bufferedWriter.write(content)
  }

def templateCode(
    projectName: String,
    environment: String,
    config: DelomatenConfig,
    code: String
): String =
  s"""from dapla_pseudo import Depseudonymize, Pseudonymize, Repseudonymize
    |from dapla_metadata.datasets.core import Datadoc
    |from datetime import date
    |import logging
    |from google.cloud import storage
    |import io
    |from pathlib import Path
    |import polars as pl
    |import sys
    |import json
    |import time
    |import random
    |import itertools

    |def with_exponential_backoff(io_action, max_total_time, base_delay=1, max_delay=60, max_retries=None):
    |  def backoff_delays():
    |      # Generate delays: base * 2^n, capped at max_delay, with jitter
    |      for n in itertools.count():
    |          raw = min(base_delay * (2 ** n), max_delay)
    |          yield raw * random.uniform(0.5, 1.5)

    |  def try_action(delays, deadline, attempt=1):
    |      try:
    |          return io_action()
    |      except Exception as e:
    |          now = time.time()
    |          if now >= deadline or (max_retries is not None and attempt > max_retries):
    |              raise e
    |          delay = next(delays)
    |          remaining = deadline - now
    |          time.sleep(min(delay, max(0, remaining)))
    |          return try_action(delays, deadline, attempt + 1)

    |  deadline = time.time() + max_total_time
    |  return try_action(backoff_delays(), deadline)

    |def read_metadata_file(file_path) -> Datadoc:
    |  def read_and_parse():
    |      return Datadoc(file_path)
    |
    |  return with_exponential_backoff(read_and_parse, max_delay=5 * 60) # 5 minutes

    |def main(file_path):
    |    try:
    |        datadoc = read_metadata_file(file_path)
    |    except Exception as e:
    |        print("Failed to read file as datadoc object:", e)
    |
    |    try:
    |        df = pl.read_parquet(file_path)
    |    except Exception as e:
    |        logging.error(f"Failed to read {file_path} from parquet into dataframe\\n\\n{e}")
    |        sys.exit(1)
    |
    |${code}
    |    metrics = json.dumps(result.metadata_details, indent=4)
    |    metadata = result.datadoc
    |    logging.info("Metrics metadata %s", metrics)
    |    final_df = result.to_polars()

    |    client = storage.Client()
    |    bucket = client.bucket("ssb-${projectName}-data-delt-delomat-${config.sharedBucket}-${environment}")
    |    filename = Path(file_path).name
    |    filename_metrics = f"{Path(file_path).stem}_METRICS.json"
    |    filename_metadata = f"{Path(file_path).stem}__DOC.json"
    |    blob_df = bucket.blob(str(Path("${config.destinationFolder}", filename)))
    |    blob_metrics = bucket.blob(str(Path("${config.destinationFolder}", filename_metrics)))
    |    blob_metadata = bucket.blob(str(Path("${config.destinationFolder}", filename_metadata)))
    |
    |    buffer = io.BytesIO()
    |    final_df.write_parquet(buffer)
    |    buffer.seek(0)

    |    blob_df.upload_from_file(buffer, content_type="application/octet-stream")
    |    logging.info(f"Result uploaded to ${config.sharedBucket}/${config.destinationFolder}/{filename}")
    |    blob_metrics.upload_from_string(metrics, content_type="application/json")
    |    logging.info(f"Metrics uploaded to ${config.sharedBucket}/${config.destinationFolder}/{filename_metrics}")
    |    blob_metadata.upload_from_string(metadata, content_type="application/json")
    |    logging.info(f"Metrics uploaded to ${config.sharedBucket}/${config.destinationFolder}/{filename_metadata}")
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
    |        .with_metadata(datadoc)
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
