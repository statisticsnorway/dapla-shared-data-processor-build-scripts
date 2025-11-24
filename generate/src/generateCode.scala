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
      config.operation match
        case PseudoOperationType.Pseudo(tasks) =>
          genPseudoTask(tasks)
        case PseudoOperationType.Depseudo(task) =>
          val columns =
            task.columns.map { c => s"\"${c}\"" }.mkString("[", ",", "]")
          s"|    result = build_and_run_depseudo(df,datadoc,${columns})".stripMargin

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
      case Array(projectDisplayName, configDataPath) =>
        generateCode(projectDisplayName, configDataPath)
      case Array(projectDisplayName, configDataPath, writeFilepath) =>
        generateCode(projectDisplayName, configDataPath, Some(writeFilepath))

/* Split project display name into project name and environment
 *
 * Example:
 *   "play-obr-test" -> ("play-obr", "test")
 */
def splitProjectDisplayName(projectDisplayName: String): (String, String) =
  val splitIndex = projectDisplayName.lastIndexOf("-")
  (
    projectDisplayName.substring(0, splitIndex),
    projectDisplayName.substring(splitIndex + 1)
  )

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
    |from dapla_metadata.datasets.utility.utils import VariableType
    |from datetime import date
    |import logging
    |from google.cloud import storage
    |import io
    |from pathlib import Path
    |from pprint import pformat
    |import polars as pl
    |import sys
    |import json
    |import time
    |import random
    |import itertools
    |from gcsfs import GCSFileSystem

    |def guard_file_exists(gcs_file_path, max_total_time, base_delay=1, max_delay=60, max_retries=None):
    |  fs = GCSFileSystem()
    |  def backoff_delays():
    |      # Generate delays: base * 2^n, capped at max_delay, with jitter
    |      for n in itertools.count():
    |          raw = min(base_delay * (2 ** n), max_delay)
    |          yield raw * random.uniform(0.5, 1.5)

    |  def try_action(delays, deadline, attempt=1):
    |      if fs.exists(gcs_file_path):
    |          return True
    |      else:
    |          now = time.time()
    |          if now >= deadline or (max_retries is not None and attempt > max_retries):
    |              error_msg = f"Could not find file: {gcs_file_path}"
    |              logging.error(error_msg)
    |              raise FileExistsError(error_msg)
    |          delay = next(delays)
    |          remaining = deadline - now
    |          time.sleep(min(delay, max(0, remaining)))
    |          return try_action(delays, deadline, attempt + 1)

    |  deadline = time.time() + max_total_time
    |  return try_action(backoff_delays(), deadline)

    |def get_decryption_algorithm(variable: VariableType)-> tuple[str, dict[str,str | None]]:
    |    \"\"\"Given a pseudonymized variable, update the depseudonymization builder with the correct algorithm and arguments.\"\"\"
    |    pseudo_metadata = variable.pseudonymization
    |    encryption_key_reference = pseudo_metadata.encryption_key_reference
    |
    |    if pseudo_metadata.encryption_algorithm == 'TINK-DAEAD':
    |        return ("default_encryption", {"custom_key": encryption_key_reference})
    |    elif pseudo_metadata.encryption_algorithm == 'TINK-FPE':
    |        if pseudo_metadata.stable_identifier_type == 'FREG_SNR' and pseudo_metadata.stable_identifier_version != None:
    |            failure_strategy: str | None = next((param["failureStrategy"] for param in pseudo_metadata.encryption_algorithm_parameters if "failureStrategy" in param), None)
    |            return("stable_id", {"custom_key": encryption_key_reference, "sid_snapshot_date": pseudo_metadata.stable_identifier_version, "on_map_failure": failure_strategy})
    |        elif pseudo_metadata.stable_identifier_type == None and pseudo_metadata.stable_identifier_version == None:
    |            return("papis_compatible_encryption", {"custom_key": encryption_key_reference})
    |
    |    raise ValueError(
    |        f\"\"\"Cannot determine depseudonymization algorithm used for variable '{variable.short_name}'. Relevant metadata from the 'pseudonymization' field:
    |
    |        {pformat(pseudo_metadata)}
    |        \"\"\"
    |    )
    |
    |def build_and_run_depseudo(df: pl.DataFrame, datadoc_metadata: Datadoc, columns: list[str]) -> Any:
    |    \"\"\"Create and then execute a depseudonymization builder block based on the datadoc metadata.\"\"\"
    |    variable_dict: dict[str, VariableType] = (
    |      {v.short_name:v for v in datadoc_metadata.datadoc_model().datadoc.variables}
    |    )
    |    variable_names = list(variable_dict.keys())
    |    for column in columns:
    |        if column not in variable_names:
    |            raise ValueError(
    |              f"Column '{column}' not found in supplied Datadoc metadata variables {variable_names}"
    |            )
    |
    |    builder = Depseudonymize.from_polars(df).with_metadata(datadoc_metadata)
    |
    |    # Dynamically build the 'Depseudonymize' block based on the datadoc pseudonymization metadata
    |    for column in columns:
    |        builder = builder.on_fields(column)
    |        match get_decryption_algorithm(variable_dict[column]):
    |            case ("default_encryption", kwargs):
    |                builder = builder.with_default_encryption(**kwargs)
    |            case ("stable_id", kwargs):
    |                builder = builder.with_stable_id(**kwargs)
    |            case ("papis_compatible_encryption", kwargs):
    |                builder = builder.with_papis_compatible_encryption(**kwargs)
    |            case else:
    |                raise ValueError(f"Unexpected match case when building depseudo block\\n{else}")
    |
    |    return builder.run()

    |def main(file_path):
    |    pure_path = Path(file_path.removeprefix("gs://"))
    |    metadata_document_path = "gs://" + str(Path(pure_path).parent / (Path(pure_path).stem + "__DOC.json"))
    |    guard_file_exists(metadata_document_path, max_total_time=60 * 5) # 5 minutes timeout
    |    datadoc = Datadoc(metadata_document_path=metadata_document_path)
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
    |    bucket = client.bucket("ssb-${projectName}-data-delt-${config.sharedBucket}-${environment}")
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
    |    logging.info(f"Metadata uploaded to ${config.sharedBucket}/${config.destinationFolder}/{filename_metadata}")
  """.stripMargin

def genPseudoTask(
    tasks: List[PseudoTask]
): String =
  val taskBlocks = tasks.map(genTaskBlock).mkString("\n")

  s"""    result = (
   |      Pseudonymize
   |        .from_polars(df)
   |        .with_metadata(datadoc)
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
      val formattedDate: String = sidSnapshotDate
        .map { date =>
          s"\"${SimpleDateFormat("yyyy-MM-dd").format(date)}\""
        }
        .getOrElse("str(date.today())")
      val mapStrat: Json = sidOnMapFailure
        .map(_.asJson)
        .getOrElse(SidMapFailureStrategy.ReturnNull.asJson)
      s"with_stable_id(custom_key=${key.asJson}, sid_snapshot_date=$formattedDate, on_map_failure=$mapStrat)"

  val columns = task.columns.map(col => s"\"$col\"").mkString(",")

  s"""        .on_fields(${columns})
  |        .${encryptAlgo}""".stripMargin
