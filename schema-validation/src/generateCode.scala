//> using scala 3.6.3
//> using dep org.typelevel::cats-core:2.13.0
//> using dep io.circe::circe-yaml:1.15.0
//> using dep io.circe::circe-generic:0.14.10
//> using dep io.circe::circe-parser:0.14.10
//> using files types.scala utils.scala
import com.schemavalidation.types.{given, *}
import com.schemavalidation.utils.*
import java.nio.file.{Files, Path, Paths}
import scala.util.{Try, Using}
import java.io.FileWriter
import java.io.BufferedWriter
import DataFrameString.*

enum DataFrameString:
  case DataFrame
  case Result

@main def generateCode(configDataPath: String): Unit =
  val configPath: Path = Paths.get(configDataPath)
  if !Files.exists(configPath) then
    println(
      s"The configuration file at ${configDataPath} does not exist".red.newlines
    )

  val config: DelomatenConfig = loadConfig(configPath)

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
    "process_shared_data.py",
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
  val outputCols: String = config.outputColumns match
    case Some(outputColumns) =>
      val columns: String = outputColumns.map(col => s"\"$col\"").mkString(",")
      s"final_df = result.to_polars().select(${columns})"
    case None => ""

  s"""from dapla_pseudo import Depseudonymize, Pseudonymize, Repseudonymize
    |import logging
    |from google.cloud import storage
    |import io
    |from pathlib import Path
    |import polars as pl
    |import sys

    |def main(file_path):
    |    try:
    |        df = pl.read_parquet(file_path)
    |    except Exception as e:
    |        logging.error(f"Failed to read {file_path} from parquet into dataframe\\n\\n{e}")
    |        sys.exit(1)
    |
    |${code}
    |    logging.info("Metadata", result.metadata_details)
    |    ${outputCols}

    |    client = storage.Client()
    |    bucket = client.bucket("${config.sharedBucket}")
    |    blob = bucket.blob(str(Path("${config.destinationFolder}", Path(file_path).name)))
    |
    |    buffer = io.BytesIO()
    |    final_df.write_parquet(buffer)
    |    buffer.seek(0)

    |    blob.upload_from_file(buffer, content_type="application/octet-stream")
    |    logging.info("Result uploaded to ${config.sharedBucket}/${config.destinationFolder}")
  """.stripMargin

def genPseudoTask(
    pseudoOperation: PseudoOperation,
    tasks: List[PseudoTask]
): DataFrameString => String =
  val pseudoOp = pseudoOperation match
    case PseudoOperation.Depseudo => "Depseudonymize"
    case PseudoOperation.Pseudo   => "Pseudonymize"
    case PseudoOperation.Repseudo => "Repseudonymize"

  val taskBlocks = tasks.map(genTaskBlock).mkString("\n")

  dataFrame =>
    val fromType = dataFrame match
      case DataFrame => "polars"
      case Result => "result"
    val dataFrameString = dataFrame match
      case DataFrame => "df"
      case Result => "result"
    s"""    result = (
    |      ${pseudoOp}
    |        .from_${fromType}(${dataFrameString})
    |${taskBlocks}
    |        .run()
    |    )""".stripMargin

def genTaskBlock(task: PseudoTask): String =
  val sidMappingArg = (for
    argMap <- task.encryptionArgs
    sidMappingDate <- argMap.get("sid_mapping_date")
  yield s"sid_snapshot_date=\"${sidMappingDate}\"").getOrElse("")

  val encryptAlgo = task.encryptionAlgorithm match
    case EncryptionAlgorithm.Default => "with_default_encryption()"
    case EncryptionAlgorithm.PapisCompatible =>
      "with_papis_compatible_encryption()"
    case EncryptionAlgorithm.SidMapping => s"with_stable_id(${sidMappingArg})"

  val columns = task.columns.map(col => s"\"$col\"").mkString(",")

  s"""        .on_fields(${columns})
  |        .${encryptAlgo}""".stripMargin
