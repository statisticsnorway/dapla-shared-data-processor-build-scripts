package com.schemavalidation.types

import cats.syntax.all.*
import io.circe.*
import io.circe.derivation.*
import io.circe.generic.semiauto.*
import scala.collection.immutable.*
import java.util.Date
import scala.util.Try
import java.text.SimpleDateFormat

enum PseudoOperation:
  case Pseudo, Depseudo // ,Repseudo, Redact

given Decoder[PseudoOperation] = ConfiguredEnumDecoder.derive(_.toUpperCase)
given Encoder[PseudoOperation] = ConfiguredEnumEncoder.derive(_.toUpperCase)

enum SidMapFailureStrategy:
  case ReturnNull, ReturnOriginal

val sidMapFailureStrategyConfig =
  Configuration.default.withTransformConstructorNames { constructor =>
    constructor.split("(?=[A-Z])").map(_.toUpperCase).mkString("_")
  }

given Decoder[SidMapFailureStrategy] =
  ConfiguredEnumDecoder.derived(using sidMapFailureStrategyConfig)
given Encoder[SidMapFailureStrategy] =
  ConfiguredEnumEncoder.derived(using sidMapFailureStrategyConfig)

enum EncryptionKey:
  case SsbCommonKey1, SsbCommonKey2, PapisCommonKey1

val encryptionKeyConfig =
  Configuration.default.withTransformConstructorNames { constructor =>
    constructor.split("(?=[A-Z0-9])").map(_.toLowerCase).mkString("-")
  }

given Decoder[EncryptionKey] =
  ConfiguredEnumDecoder.derived(using encryptionKeyConfig)
given Encoder[EncryptionKey] =
  ConfiguredEnumEncoder.derived(using encryptionKeyConfig)

enum EncryptionAlgorithm:
  case Default(encryptionKey: EncryptionKey)
  case PapisCompatible(encryptionKey: EncryptionKey)
  case SidMapping(
      encryptionKey: EncryptionKey,
      sidSnapshotDate: Option[Date],
      sidOnMapFailure: Option[SidMapFailureStrategy]
  )

given Decoder[Date] = Decoder.decodeString.emapTry { s =>
  val format = SimpleDateFormat("yyyy-MM-dd")
  Try(format.parse(s))
}

given Encoder[Date] = Encoder.instance { (date: Date) =>
  Json.fromString(SimpleDateFormat("yyyy-MM-dd").format(date))
}

given Decoder[EncryptionAlgorithm] = Decoder.instance { (c: HCursor) =>
  import EncryptionKey.*
  import EncryptionAlgorithm.*
  for
    algorithm <- c.downField("algorithm").as[String]
    key <- c
      .getOrElse("key")(
        if algorithm == "default" then SsbCommonKey1 else PapisCommonKey1
      )
    sidSnapshotDate <- c.downField("sid_snapshot_date").as[Option[Date]]
    sidOnMapFailure <- c
      .downField("sid_on_map_failure")
      .as[Option[SidMapFailureStrategy]]
    decode <- (algorithm, key, sidSnapshotDate, sidOnMapFailure) match
      case ("sid_mapping", key, snapshotDate, mapFail) =>
        Right(SidMapping(key, snapshotDate, mapFail))
      case ("papis_compatible", key, _, _) => Right(PapisCompatible(key))
      case ("default", key, _, _)          => Right(Default(key))
      case _ =>
        Left(
          DecodingFailure("Failed to decode EncryptionAlgorithm", List.empty)
        )
  yield decode
}

case class PseudoTask(
    name: String,
    columns: List[String],
    pseudoOperation: PseudoOperation,
    encryption: EncryptionAlgorithm
)

given Decoder[HashMap[String, String]] with
  def apply(c: HCursor): Decoder.Result[HashMap[String, String]] =
    for listOfMaps <- c.as[List[Map[String, String]]]
    yield HashMap.from(listOfMaps.flatten)

given Decoder[PseudoTask] = ConfiguredDecoder.derived(using
  Configuration.default.withSnakeCaseMemberNames
)

case class DelomatenConfig(
    sharedBucket: String,
    sourceFolderPrefix: String,
    destinationFolder: String,
    memorySize: Int,
    pseudo: List[PseudoTask]
)
given Decoder[DelomatenConfig] = ConfiguredDecoder.derived(using
  Configuration.default.withSnakeCaseMemberNames
)
