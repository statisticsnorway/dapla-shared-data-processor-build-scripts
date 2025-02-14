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

@main def generateCode(configDataPath: String): Unit =
  val configPath: Path = Paths.get(configDataPath)
  if !Files.exists(configPath) then
    println(
      s"The configuration file at ${configDataPath} does not exist".red.newlines
    )

  val config: DelomatenConfig = loadConfig(configPath)
  val destinationFolder: String =
    configPath.getParent().getFileName().toString()

  val pseudoTasks: String =
    config.pseudo
      .groupBy(_.pseudoOperation)
      .map(genPseudoTask)
      .zip(LazyList("df") ++ LazyList.continually("result"))
      .map(_(_))
      .mkString("\n\n")

  val code = templateCode(destinationFolder, config, pseudoTasks)

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
    destinationFolder: String,
    config: DelomatenConfig,
    code: String
): String =
  val outputCols: String = config.outputColumns match
    case Some(outputColumns) =>
      val columns: String = outputColumns.map(col => s"\"$col\"").mkString(",")
      s"final_df = result.to_polars().select(${columns})"
    case None => ""

  s"""from dapla_pseudo import Depseudonymize, Pseudonymize, Reseudonymize
    |from google.cloud import storage
    |import io
    |from pathlib import Path
    |import polars as pl

    |def main(file_path):
    |    df = pl.read_parquet(file_path)
    |${code}

    |    ${outputCols}

    |    client = storage.Client()
    |    bucket = client.bucket("${config.sharedBucket}")
    |    blob = bucket.blob(str(Path("${destinationFolder}", Path(file_path).name)))
    |
    |    buffer = io.BytesIO()
    |    final_df.write_parquet(buffer)
    |    buffer.seek(0)

    |    blob.upload_from_file(buffer, content_type="application/octet-stream")
  """.stripMargin

def genPseudoTask(
    pseudoOperation: PseudoOperation,
    tasks: List[PseudoTask]
): String => String =
  val pseudoOp = pseudoOperation match
    case PseudoOperation.Depseudo => "Depseudonymize"
    case PseudoOperation.Pseudo   => "Pseudonymize"
    case PseudoOperation.Repseudo => "Reseudonymize"

  val taskBlocks = tasks.map(genTaskBlock).mkString("\n")

  dataFrame => s"""    result = (
    |      ${pseudoOp}
    |        .from_polars(${dataFrame})
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
